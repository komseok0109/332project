package machine

import com.typesafe.scalalogging.LazyLogging
import io.grpc._
import message._
import java.io._

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic._
import scala.concurrent._

class ShuffleServer(executionContext: ExecutionContext, port: Int, outputDirectory: String,
                    myRange: (String, String), totalWorkerNum: Int, workerID: Int)(implicit ec: ExecutionContext)
  extends LazyLogging { self =>
  private val server: Server = ServerBuilder.forPort(port)
    .addService(ShufflingMessageGrpc.bindService(new ShuffleMessageImpl, executionContext))
    .asInstanceOf[ServerBuilder[_]]
    .build()
  private val ackLatch = new CountDownLatch(totalWorkerNum - 1)

  def start(): Unit = {
    server.start()
    logger.info("Shuffling server started, listening on " + port)
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class ShuffleMessageImpl extends ShufflingMessageGrpc.ShufflingMessage {
    override def sendDataToWorker(request: SendDataRequest): Future[EmptyAckMsg] = {
      val directory = new File(outputDirectory)
      if (!directory.exists()) {
        directory.mkdirs()
      }
      try {
        val writer = new BufferedWriter(new FileWriter(new File(directory, s"${request.fileName}"), true))
        request.data.foreach { line =>
          if (line.splitAt(10)._1 < myRange._1 || line.splitAt(10)._1 > myRange._2)
            logger.warn(s"Line '$line' is out of range: ${myRange._1} to ${myRange._2}")
          writer.write(line)
          writer.newLine()
        }
        writer.close()
        Future.successful(EmptyAckMsg())
      } catch {
        case e: AssertionError =>
          logger.error(s"Assertion failed: ${e.getMessage}")
          Future.failed(e)
        case e: Exception =>
          logger.error(s"Unexpected error: ${e.getMessage}")
          Future.failed(e)
      }
    }
    override def shuffleAck(request: ShuffleAckRequest): Future[EmptyAckMsg] = {
      logger.info(s"Worker ${request.source} has sent all data to worker $workerID")
      ackLatch.countDown()
      ackLatch.await()
      server.shutdown()
      Future.successful(EmptyAckMsg())
    }
  }
}