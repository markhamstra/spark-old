package spark.network

import java.nio.ByteBuffer

private[spark] object SenderTest {

  def main(args: Array[String]) {
    
    if (args.length < 2) {
      println("Usage: SenderTest <target host> <target port>")
      System.exit(1)
    }
   
    val targetHost = args(0)
    val targetPort = args(1).toInt
    val targetConnectionManagerId = new ConnectionManagerId(targetHost, targetPort)

    val manager = new ConnectionManager(0)
    println("Started connection manager with id = " + manager.id)

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => { 
      println("Received [" + msg + "] from [" + id + "]")
      None
    })
  
    val size =  100 * 1024  * 1024 
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val targetServer = args(0)

    val count = 100
    (0 until count).foreach(i => {
      val dataMessage = Message.createBufferMessage(buffer.duplicate)
      val startTime = System.currentTimeMillis  
      /*println("Started timer at " + startTime)*/
      val responseStr = manager.sendMessageReliablySync(targetConnectionManagerId, dataMessage) match {
        case Some(response) =>
          val buffer = response.asInstanceOf[BufferMessage].buffers(0)
          new String(buffer.array)
        case None => "none"
      }
      val finishTime = System.currentTimeMillis
      val mb = size / 1024.0 / 1024.0
      val ms = finishTime - startTime
      /*val resultStr = "Sent " + mb + " MB " + targetServer + " in " + ms + " ms at " + (mb / ms * 1000.0) + " MB/s"*/
      val resultStr = "Sent " + mb + " MB " + targetServer + " in " + ms + " ms (" +  (mb / ms * 1000.0).toInt + "MB/s) | Response = " + responseStr
      println(resultStr)
    })
  }
}

