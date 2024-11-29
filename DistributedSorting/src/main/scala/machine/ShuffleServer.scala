package machine

import com.typesafe.scalalogging.LazyLogging
import io.grpc._
import message._
import java.io._
import com.google.protobuf.ByteString
import java.util.concurrent.CountDownLatch
import scala.concurrent._

class ShuffleServer(executionContext: ExecutionContext, port: Int, outputDirectory: String,
                    myRange: (ByteString,  ByteString), totalWorkerNum: Int, workerID: Int)
                   (implicit ec: ExecutionContext)
  extends LazyLogging { self =>
  private val server: Server = ServerBuilder.forPort(port)
    .addService(ShufflingMessageGrpc.bindService(new ShuffleMessageImpl, executionContext))
    .asInstanceOf[ServerBuilder[_]]
    .build()
  private val ackLatch = new CountDownLatch(totalWorkerNum)

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
      logger.info("Received Data")
      val directory = new File(outputDirectory)
      if (!directory.exists()) {
        directory.mkdirs()
      }
      try {
        val file = new File(directory, s"${request.fileName}")
        val outputStream = new BufferedOutputStream(new FileOutputStream(file, true))
        request.data.foreach { line =>
          outputStream.write(line.toByteArray)
        }
        outputStream.close()
        logger.info("Saved All Data")
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