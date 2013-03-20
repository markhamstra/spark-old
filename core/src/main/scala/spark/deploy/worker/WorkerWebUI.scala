package spark.deploy.worker

import akka.actor.{ActorRef, ActorContext}
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import spray.routing.Directives
import spray.httpx.TwirlSupport._
import spray.httpx.SprayJsonSupport._
import spray.http.MediaTypes._

import spark.deploy.{WorkerState, RequestWorkerState}
import spark.deploy.JsonProtocol._

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(worker: ActorRef)(implicit val context: ActorContext) extends Directives {
  import context.dispatcher

  val actorSystem         = context.system
  val RESOURCE_DIR        = "spark/deploy/worker/webui"
  val STATIC_RESOURCE_DIR = "spark/deploy/static"


  implicit val timeout = Timeout(10 seconds)

  val handler = {
    get {
      (path("")  & parameters('format ?)) {
        case Some(js) if js.equalsIgnoreCase("json") => {
          val future = (worker ? RequestWorkerState).mapTo[WorkerState]
          respondWithMediaType(`application/json`) { ctx =>
            ctx.complete(future)
          }
        }
        case _ =>
          complete {
            val future = (worker ? RequestWorkerState).mapTo[WorkerState]
            future.map { workerState =>
              spark.deploy.worker.html.index(workerState)
            }
          }
      } ~
      path("log") {
        parameters("appId", "executorId", "logType") { (appId, executorId, logType) =>
          respondWithMediaType(cc.spray.http.MediaTypes.`text/plain`) {
            getFromFileName("work/" + appId + "/" + executorId + "/" + logType)
          }
        }
      } ~
      pathPrefix("static") {
        getFromResourceDirectory(STATIC_RESOURCE_DIR)
      } ~
      getFromResourceDirectory(RESOURCE_DIR)
    }
  }
}
