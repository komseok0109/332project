package machine

import com.typesafe.scalalogging.LazyLogging
import io.grpc._
import message._
import utils._

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
  private val registeredWorkersIP: TrieMap[Int, String] = TrieMap()
  private val workerIDCounter: AtomicInteger = new AtomicInteger(1)
  private val receivedSamples: TrieMap[Int, Seq[String]] = TrieMap()

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

    override def registerWorker(request: RegisterWorkerRequest): Future[RegisterWorkerReply] = {
      if (registeredWorkersIP.size >= numWorkers) {
        logger.error(s"Worker registration failed: Maximum worker limit ($numWorkers) reached.")
        Future.failed(new IllegalStateException(s"Maximum worker limit ($numWorkers) reached."))
      } else {
        val workerIDToAssign = workerIDCounter.getAndIncrement()
        registeredWorkersIP.put(workerIDToAssign, request.workerIP)
        logger.info(s"Worker(${request.workerIP}) has registered with ID $workerIDToAssign")
        Future.successful(RegisterWorkerReply(totalWorkerCount = numWorkers, workerID = workerIDToAssign))
      }
    }
    override def calculatePivots(request: CalculatePivotRequest): Future[CalculatePivotReply] = {
      receivedSamples.put(request.workerID, request.sampleData)
      logger.info(s"Sample data from ${request.workerID} received")
      samplingLatch.countDown()
      samplingLatch.await()
      assert(receivedSamples.size == numWorkers, s"Registered Workers < # of Workers")
      assert(samplingLatch.getCount == 0)
      val sortedSamples = receivedSamples.values.flatten.toList.sorted
      val rangeStep = sortedSamples.length / numWorkers
      val keyRanges: Seq[WorkerIDKeyRangeMapping] = for {
        i <- 0 until numWorkers
        startKey = if (i == 0) " " * 10 else sortedSamples((i - 1) * rangeStep)
        endKey = if (i == numWorkers - 1) "~" * 10 else sortedSamples(i * rangeStep)
      } yield WorkerIDKeyRangeMapping(i + 1, startKey, endKey)

      Future.successful(CalculatePivotReply(workerIPs = registeredWorkersIP.toMap, keyRangeMapping = keyRanges))
    }
    override def partitionEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = {
      logger.info(s"Worker ${request.workerID} has notified that partitioning is done")
      partitionAckLatch.countDown()
      partitionAckLatch.await()
      assert(partitionAckLatch.getCount == 0)
      Future.successful(EmptyAckMsg())
    }

    override def shufflingEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = {
      logger.info(s"Worker ${request.workerID} has notified that shuffling is done")
      shufflingAckLatch.countDown()
      shufflingAckLatch.await()
      assert(shufflingAckLatch.getCount == 0, "latch's value is not 0")
      Future.successful(EmptyAckMsg())
    }

    override def mergeEndMsg(request: PhaseCompleteNotification): Future[EmptyAckMsg] = {
      mergeAckLatch.countDown()
      mergeAckLatch.await()
      assert(mergeAckLatch.getCount == 0)
      server.shutdown()
      Future.successful(EmptyAckMsg())
    }
  }

  def printResult(): Unit = {
    logger.info("All workers have successfully completed!")
    println(IPUtils.getMachineIP + s":${PORT}")
    registeredWorkersIP.values.foreach(ip => print(ip + " "))
    println()
  }
}