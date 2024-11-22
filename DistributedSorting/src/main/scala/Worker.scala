import io.grpc._
import com.typesafe.scalalogging.LazyLogging
import message._
import utils._
import scala.concurrent._
import scala.collection.concurrent._
import scala.io.Source
import scala.util.Random


object Worker extends LazyLogging {
  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
    ArgumentParser.parseWorkerArgs(args) match {
      case Some(config) =>
        logger.info("Starting worker with configuration: " + config)
        val worker = new Worker(config.masterIP.split(":")(0), config.masterIP.split(":")(1).toInt,
         config.inputDirectories, config.outputDirectory)(ec)
        worker.run()
      case None =>
        logger.error("Invalid arguments.")
    }
  }
}

class Worker(masterHost: String, masterPort: Int,
             inputDirectories: Seq[String], outputDirectory: String)(implicit ec: ExecutionContext)
  extends LazyLogging {
  private val channel = ManagedChannelBuilder
    .forAddress(masterHost, masterPort).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
  private val stub = MessageGrpc.blockingStub(channel)
  //lazy private val filePaths = IOUtils.getFilePathsFromDirectories(inputDirectories.toList)
  private var workerID: Option[Int] = None
  private var totalWorkerCount: Option[Int] = None
  private val registeredWorkersIP: TrieMap[Int, String] = TrieMap()
  private val ID2Ranges: TrieMap[Int, (String, String)] = TrieMap()
  private val Range2IDs: TrieMap[(String, String), Int] = TrieMap()

  def run(): Unit = {
    logger.info("Connect to Server: " + masterHost + ":" + masterPort)
    try {
      registerToMaster()
    } catch {
      case e: Exception =>
        logger.error(s"Registration Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    try {
      sendSamplesToMaster()
    } catch {
      case e: Exception =>
        logger.error(s"Key Range Calculation Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    try {
      notifyMasterPartitionDone()
    } catch {
      case e: Exception =>
        logger.error(s"Partition Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    try {
      notifyMasterMergingDone()
    } catch {
      case e: Exception =>
        logger.error(s"Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    shutDownChannel()
  }

  private def shutDownChannel(): Unit = {
    channel.shutdownNow()
  }

  private def registerToMaster(): Unit = {
    IPUtils.getMachineIP match {
      case "unknown" => throw new RuntimeException("Failed to get IP")
      case ip =>
        val request = RegisterWorkerRequest(workerIP = ip)
        try {
          val reply = stub.registerWorker(request)
          workerID = Some(reply.workerID)
          totalWorkerCount = Some(reply.totalWorkerCount)
          logger.info(s"Successfully registered with worker ID: ${reply.workerID} and total workers: ${reply.totalWorkerCount}")
        } catch {
          case e: Exception =>
            logger.error(s"Failed to register worker: ${e.getMessage}")
            throw new RuntimeException("Worker registration failed, terminating program")
        }
    }
  }

  private def sendSamplesToMaster(): Unit = {
    assert(workerID.nonEmpty && totalWorkerCount.nonEmpty)
    try {
      val request = CalculatePivotRequest(workerID = workerID.get, sampleData = getRandomSample())
      val reply = stub.calculatePivots(request)
      reply.workerIPs.foreach( mapping => registeredWorkersIP.put(mapping._1, mapping._2))
      reply.keyRangeMapping.foreach(mapping => {
        ID2Ranges.put(mapping.workerID, (mapping.startKey, mapping.endKey))
        Range2IDs.put((mapping.startKey, mapping.endKey), mapping.workerID)
      })
      logger.info(s"Successfully get ${registeredWorkersIP} and ${ID2Ranges(workerID.get)}")
    } catch {
      case e: Exception =>
        logger.error(s"Error during pivot calculation: ${e.getMessage}")
        throw new RuntimeException("Error processing pivot calculation reply")
    }
  }

  private def notifyMasterPartitionDone(): Unit = {
    assert(workerID.nonEmpty)
    try {
      val request = PhaseCompleteNotification(workerID = workerID.get)
      val reply = stub.partitionEndMsg(request)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to receive partition acknowledgement: ${e.getMessage}")
        throw new RuntimeException("Ack error")
    }
  }

  private def notifyMasterMergingDone(): Unit = {
    assert(workerID.nonEmpty)
    try {
      val request = PhaseCompleteNotification(workerID = workerID.get)
      val reply = stub.mergeEndMsg(request)
    } catch {
      case e: Exception =>
        logger.error(s"Error during termination: ${e.getMessage}")
        throw new RuntimeException("Termination Error")
    }
  }

  private def getRandomSample(sampleSize: Int = 25): Seq[String] = {
    val source = Source.fromFile("/home/white/64/partition2")
    try {
      val lines = source.getLines().toSeq
      if (lines.size < sampleSize)
        throw new IllegalArgumentException(s"File has fewer than $sampleSize lines")
      val randomLines = Random.shuffle(lines).take(sampleSize)
      randomLines.map { line =>
        val key = line.substring(0, 10).trim
        key
      }.filter(_.nonEmpty)
    } finally {
      source.close()
    }
  }

}


