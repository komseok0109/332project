import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future}
import message._

object Master {
  def main(args: Array[String]): Unit  = {
    val server = new Master(ExecutionContext.global)
    println(args(1))
    server.start()
    server.blockUntilShutdown()
  }
}

class Master(executionContext: ExecutionContext) { self =>
  private[this] var server: io.grpc.Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort(50051)
      .addService(MessageGrpc.bindService(new MessageImpl, executionContext))
      .build()
      .start()
    println("Server started, listening on " + 50051)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
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