package machine

import com.typesafe.scalalogging.LazyLogging
import io.grpc._
import message._
import utils._
import com.google.protobuf.ByteString
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic._
import scala.collection.concurrent._
import scala.concurrent._

object Master extends LazyLogging {
  def main(args: Array[String]): Unit  = {
    ArgumentParser.parseMasterArgs(args) match {
      case Some(config) =>
        logger.info("Starting master with configuration: " + config)
        val master = new Master(ExecutionContext.global, config.numWorkers)(ExecutionContext.global)
        master.start()
        master.blockUntilShutdown()
        master.printResult()
      case None =>
        logger.error("Invalid arguments.")
    }
  }
}

class Master(executionContext: ExecutionContext, numWorkers: Int)(implicit ec: ExecutionContext)
  extends LazyLogging { self =>
  private val PORT = 50051
  private[this] val server: io.grpc.Server = ServerBuilder.forPort(PORT)
    .addService(MessageGrpc.bindService(new MessageImpl, executionContext))
    .asInstanceOf[ServerBuilder[_]]
    .build()
  private val registeredWorkersIP: TrieMap[Int, ByteString] = TrieMap()
  private val workerIDCounter: AtomicInteger = new AtomicInteger(1)
  private val receivedSamples: TrieMap[Int, Seq[ByteString]] = TrieMap()

  def start(): Unit = {
    server.start()
    logger.info("Server started, listening on " + PORT)
    sys.addShutdownHook {
      logger.info("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      logger.info("*** server shut down")
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
    private val samplingLatch: CountDownLatch = new CountDownLatch(numWorkers)
    private val mergeAckLatch: CountDownLatch = new CountDownLatch(numWorkers)
    private val partitionAckLatch: CountDownLatch = new CountDownLatch(numWorkers)
    private val shufflingAckLatch: CountDownLatch = new CountDownLatch(numWorkers)

    override def registerWorker(request: RegisterWorkerRequest): Future[RegisterWorkerReply] = ???
    override def calculatePivots(request: CalculatePivotRequest): Future[CalculatePivotReply] = ???

    override def partitionEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???

    override def shufflingEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???

    override def mergeEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = ???
  }

  def printResult(): Unit = {
    logger.info("All workers have successfully completed!")
    println(IPUtils.getMachineIP + s":${PORT}")
    registeredWorkersIP.values.foreach(ip => print(ip + " "))
    println()
  }
}