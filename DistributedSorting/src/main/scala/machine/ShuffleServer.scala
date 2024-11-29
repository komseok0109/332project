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
    override def sendDataToWorker(request: SendDataRequest): Future[EmptyAckMsg] = ???
    override def shuffleAck(request: ShuffleAckRequest): Future[EmptyAckMsg] = ???
  }
}