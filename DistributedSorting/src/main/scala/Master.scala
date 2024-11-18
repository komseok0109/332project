import io.grpc._
import java.util.concurrent.atomic._
import scala.collection.concurrent._
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.LazyLogging
import message._
import utils._

import java.util.concurrent.atomic.AtomicInteger

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
  private val registeredWorkersIP: TrieMap[Int, String] = TrieMap()
  private val workerIDCounter = new AtomicInteger(0)

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

  private class MessageImpl extends MessageGrpc.Message {
    override def registerWorker(request: RegisterWorkerRequest): Future[RegisterWorkerReply] = {
      val workerIDToAssign = workerIDCounter.getAndIncrement()
      registeredWorkersIP.put(workerIDToAssign, request.workerIP)
      logger.info(s"Worker(${request.workerIP}) has registered with ID $workerIDToAssign")
      Future.successful(RegisterWorkerReply(totalWorkerCount = numWorkers, workerID = workerIDToAssign))
    }
    override def calculatePivots(request: CalculatePivotRequest): Future[CalculatePivotReply] = ???
    override def partitionEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???
    override def startShuffling(request: StartShufflingRequest): Future[StartShufflingReply] = ???
    override def mergeEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???
  }
}