package spark.deploy.yarn

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import spark.{Logging, Utils}

class WorkerRunnable(container: Container, conf: Configuration, masterAddress: String,
    slaveId: String, hostname: String, workerMemory: Int, workerCores: Int) 
    extends Runnable with Logging {
  
  var rpc: YarnRPC = YarnRPC.create(conf)
  var cm: ContainerManager = null
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  
  def run = {
    logInfo("Starting Worker Container")
    cm = connectToCM
    startContainer
  }
  
  def startContainer = {
    logInfo("Setting up ContainerLaunchContext")
    
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]
    
    ctx.setContainerId(container.getId())
    ctx.setResource(container.getResource())
    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)
    
    val env = prepareEnvironment
    ctx.setEnvironment(env)
    
    // Extra options for the JVM
    var JAVA_OPTS = ""
    // Set the JVM memory
    val workerMemoryString = workerMemory + "m"
    JAVA_OPTS += "-Xms" + workerMemoryString + " -Xmx" + workerMemoryString + " "
    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }
    // Commenting it out for now - so that people can refer to the properties if required. Remove it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence if there are multiple containers in same
    // node, spark gc effects all other containers performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in multi-tenant environments. Not sure how default java gc behaves if it is
    // limited to subset of cores on a node.
/*
    else {
      // If no java_opts specified, default to using -XX:+CMSIncrementalMode
      // It might be possible that other modes/config is being done in SPARK_JAVA_OPTS, so we dont want to mess with it.
      // In our expts, using (default) throughput collector has severe perf ramnifications in multi-tennent machines
      // The options are based on
      // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use%20the%20Concurrent%20Low%20Pause%20Collector|outline
      JAVA_OPTS += " -XX:+UseConcMarkSweepGC "
      JAVA_OPTS += " -XX:+CMSIncrementalMode "
      JAVA_OPTS += " -XX:+CMSIncrementalPacing "
      JAVA_OPTS += " -XX:CMSIncrementalDutyCycleMin=0 "
      JAVA_OPTS += " -XX:CMSIncrementalDutyCycle=10 "
    }
*/

    ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName())
    val commands = List[String]("java " +
      " -server " +
      // Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
      // Not killing the task leaves various aspects of the worker and (to some extent) the jvm in an inconsistent state.
      // TODO: If the OOM is not recoverable by rescheduling it on different node, then do 'something' to fail job ... akin to blacklisting trackers in mapred ?
      " -XX:OnOutOfMemoryError='kill %p' " +
      JAVA_OPTS +
      " spark.executor.StandaloneExecutorBackend " +
      masterAddress + " " +
      slaveId + " " +
      hostname + " " +
      workerCores +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    logInfo("Setting up worker with commands: " + commands)
    ctx.setCommands(commands)
    
    // Send the start request to the ContainerManager
    val startReq = Records.newRecord(classOf[StartContainerRequest])
    .asInstanceOf[StartContainerRequest]
    startReq.setContainerLaunchContext(ctx)
    cm.startContainer(startReq)
  }
  
  
  def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    
    // Spark JAR
    val sparkJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    sparkJarResource.setType(LocalResourceType.FILE)
    sparkJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    sparkJarResource.setResource(ConverterUtils.getYarnUrlFromURI(
      new URI(System.getenv("SPARK_YARN_JAR_PATH"))))
    sparkJarResource.setTimestamp(System.getenv("SPARK_YARN_JAR_TIMESTAMP").toLong)
    sparkJarResource.setSize(System.getenv("SPARK_YARN_JAR_SIZE").toLong)
    locaResources("spark.jar") = sparkJarResource
    // User JAR
    val userJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    userJarResource.setType(LocalResourceType.FILE)
    userJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    userJarResource.setResource(ConverterUtils.getYarnUrlFromURI(
      new URI(System.getenv("SPARK_YARN_USERJAR_PATH"))))
    userJarResource.setTimestamp(System.getenv("SPARK_YARN_USERJAR_TIMESTAMP").toLong)
    userJarResource.setSize(System.getenv("SPARK_YARN_USERJAR_SIZE").toLong)
    locaResources("app.jar") = userJarResource

    // Log4j conf - if available
    if (System.getenv("SPARK_YARN_LOG4J_PATH") != null) {
      val log4jConfResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
      log4jConfResource.setType(LocalResourceType.FILE)
      log4jConfResource.setVisibility(LocalResourceVisibility.APPLICATION)
      log4jConfResource.setResource(ConverterUtils.getYarnUrlFromURI(
        new URI(System.getenv("SPARK_YARN_LOG4J_PATH"))))
      log4jConfResource.setTimestamp(System.getenv("SPARK_YARN_LOG4J_TIMESTAMP").toLong)
      log4jConfResource.setSize(System.getenv("SPARK_YARN_LOG4J_SIZE").toLong)
      locaResources("log4j.properties") = log4jConfResource
    }

    
    logInfo("Prepared Local resources " + locaResources)
    return locaResources
  }
  
  def prepareEnvironment: HashMap[String, String] = {
    val env = new HashMap[String, String]()
    // should we add this ?
    Apps.addToEnvironment(env, Environment.USER.name, Utils.getUserNameFromEnvironment())

    // If log4j present, ensure ours overrides all others
    if (System.getenv("SPARK_YARN_LOG4J_PATH") != null) {
      // Which is correct ?
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./log4j.properties")
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./")
    }

    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./*")
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "$CLASSPATH")
    Client.populateHadoopClasspath(yarnConf, env)

    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    return env
  }
  
  def connectToCM: ContainerManager = {
    val cmHostPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
    val cmAddress = NetUtils.createSocketAddr(cmHostPortStr)
    logInfo("Connecting to ContainerManager at " + cmHostPortStr)
    return rpc.getProxy(classOf[ContainerManager], cmAddress, conf).asInstanceOf[ContainerManager]
  }
  
}
