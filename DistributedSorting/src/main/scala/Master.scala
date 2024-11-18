import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.LazyLogging
import message._
import utils._

object Master extends LazyLogging {
  def main(args: Array[String]): Unit  = {
    ArgumentParser.parseMasterArgs(args) match {
      case Some(config) =>
        logger.info("Starting master with configuration: " + config)
        val master = new Master(ExecutionContext.global, config.numWorkers)
        master.start()
        master.blockUntilShutdown()
      case None =>
        logger.error("Invalid arguments.")
    }
  }
}

class Master(executionContext: ExecutionContext, numWorkers: Int) extends LazyLogging { self =>
  private[this] var server: io.grpc.Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort(50051)
      .addService(MessageGrpc.bindService(new MessageImpl, executionContext))
      .build()
      .start()
    logger.info("Server started, listening on " + 50051)
    sys.addShutdownHook {
      logger.warn("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      logger.warn("*** server shut down")
    }
  }


  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class MessageImpl extends MessageGrpc.Message {
    override def registerWorker(request: RegisterWorkerRequest): Future[RegisterWorkerReply] = ???
    override def calculatePivots(request: CalculatePivotRequest): Future[CalculatePivotReply] = ???
    override def partitionEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???
    override def startShuffling(request: StartShufflingRequest): Future[StartShufflingReply] = ???
    override def sendDataToWorker(request: SendDataRequest): Future[EmptyAckMsg] = ???
    override def shuffleAck(request: ShuffleAckRequest): Future[EmptyAckMsg] = ???
    override def mergeEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???
  }
}