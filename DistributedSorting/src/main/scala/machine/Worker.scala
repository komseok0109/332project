package machine

import com.typesafe.scalalogging.LazyLogging
import io.grpc._
import message._
import utils._
import functionality._
import scala.collection.concurrent._
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.concurrent.Executors
import com.google.protobuf.ByteString
import java.io.{FileInputStream, BufferedInputStream}

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
  lazy private val filePaths = IOUtils.getFilePathsFromDirectories(inputDirectories.toList)
  private var workerID: Option[Int] = None
  private var totalWorkerCount: Option[Int] = None
  private val registeredWorkersIP: TrieMap[Int, String] = TrieMap()
  private val ID2Ranges: TrieMap[Int, (ByteString, ByteString)] = TrieMap()
  private val Range2IDs: TrieMap[(ByteString, ByteString), Int] = TrieMap()
  private lazy val server: ShuffleServer = new ShuffleServer(executionContext = ec, port = masterPort + workerID.get,
    outputDirectory = outputDirectory, myRange = ID2Ranges(workerID.get),
    totalWorkerNum = totalWorkerCount.get, workerID = workerID.get)
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
      val sortRequired = true
      SortAndPartition.openFileAndProcessing(filePaths, ID2Ranges.toList, outputDirectory,
        workerID.get, totalWorkerCount.get, sortRequired)
      logger.info("Sorting and partitioning have successfully completed")
    } catch {
      case e: Exception =>
        logger.error(s"Partition Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    try {
      server.start()
      notifyMasterPartitionDone()
      logger.info("After partitioning done, shuffling server has started")
    } catch {
      case e: Exception =>
        logger.error(s"Server Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    try {
      shuffle()
      notifyMasterShufflingDone()
      deleteTempDirectories()
    } catch {
      case e: Exception =>
        logger.error(s"Shuffling Error: ${e.getMessage}")
        shutDownChannel()
        deleteTempDirectories()
        System.exit(1)
    }
    val tempFiles = IOUtils.getFilePathsFromDirectories(List(outputDirectory))
    try {
      val rangeMapping = Merge.getRangeMapping(tempFiles, ID2Ranges(workerID.get))
      rangeMapping.foreach(s => logger.info(s"${s}"))
      val sortRequired = false
        SortAndPartition.openFileAndProcessing(tempFiles, rangeMapping, outputDirectory,
        workerID.get, 4, sortRequired)
    } catch {
      case e: Exception => logger.error(s"Partition before merge error: ${e.getMessage}")
        IOUtils.deleteFiles(tempFiles)
        shutDownChannel()
        System.exit(1)
    }
    try {
      IOUtils.deleteFiles(tempFiles)
      Merge.merge(outputDirectory, workerID.get)
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
          logger.info(s"Successfully registered with worker ID: ${reply.workerID} " +
            s"and total workers: ${reply.totalWorkerCount}")
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
      val request = CalculatePivotRequest(workerID = workerID.get, sampleData = Sample.sampleFile(filePaths))
      val reply = stub.calculatePivots(request)
      reply.workerIPs.foreach( mapping => registeredWorkersIP.put(mapping._1, mapping._2))
      reply.keyRangeMapping.foreach(mapping => {
        ID2Ranges.put(mapping.workerID, (mapping.startKey, mapping.endKey))
        Range2IDs.put((mapping.startKey, mapping.endKey), mapping.workerID)
      })
      logger.info(s"Successfully received partition range: ${ID2Ranges(workerID.get)}.")
    } catch {
      case e: Exception =>
        logger.error(s"Error during pivot calculation: ${e.getMessage}")
        throw new RuntimeException("Error processing pivot calculation reply")
    }
  }

  private def shuffle(): Unit = {
    val MAX_BATCH_SIZE = 5
    val threadPool = Executors.newFixedThreadPool(MAX_BATCH_SIZE)
    val customExecutionContext = ExecutionContext.fromExecutor(threadPool)
    try {
      val futureList = {
        (1 to totalWorkerCount.get).map { i: Int =>
          Future {
            val filePaths = IOUtils.getFilePathsFromDirectories(List(outputDirectory + s"/${i}"))
            val channel = ManagedChannelBuilder.forAddress(registeredWorkersIP(i), masterPort + i)
              .usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build()
            val stub = ShufflingMessageGrpc.blockingStub(channel)
            try {
              logger.info(s"Worker ${workerID.get}: Shuffling started for worker $i.")
              filePaths.foreach(filePath => {
                if (i == workerID.get) {
                  try {
                    val sourcePath = Paths.get(filePath)
                    Files.copy(sourcePath, Paths.get(s"$outputDirectory/${sourcePath.getFileName}"),
                      StandardCopyOption.REPLACE_EXISTING)
                  } catch {
                    case e: Exception => logger.error(s"Error Copying File ${filePath}: ${e.getMessage}")
                  }
                } else {
                  try {
                    val source = new BufferedInputStream(new FileInputStream(filePath))
                    shuffleData(stub, source, i, Paths.get(filePath).getFileName.toString)
                    source.close()
                  } catch {
                    case e: Exception => logger.error(s"Error Sending File ${filePath}: ${e.getMessage}")
                  }
                }
              })
              try {
                val request = ShuffleAckRequest(source = workerID.get)
                stub.shuffleAck(request)
              } catch {
                case e: Exception => logger.error(s"Error shuffling acknowledgement: ${e.getMessage}")
              }
            } catch {
              case e: Exception =>
                logger.error(s"Worker ${workerID.get}: Error during shuffle operation for worker $i, ${e.getMessage}")
            } finally {
              channel.shutdown()
              logger.info(s"Worker ${workerID.get}: Channel shut down for worker $i.")
            }
          }(customExecutionContext)
        }
      }
      val batches = futureList.grouped(MAX_BATCH_SIZE)
      batches.foreach { batch =>
        Await.result(Future.sequence(batch), Duration.Inf)
      }
    } finally {
      threadPool.shutdown()
    }
  }

  private def shuffleData(stub: ShufflingMessageGrpc.ShufflingMessageBlockingStub, source: BufferedInputStream,
                          dest: Int, fileName: String): Unit = {
    try {
      val CHUNK_SIZE = 4000000
      val buffer = new Array[Byte](CHUNK_SIZE)
      var bytesRead = 0
      while ({ bytesRead = source.read(buffer); bytesRead != -1 }) {
        val chunkData = ByteString.copyFrom(buffer, 0, bytesRead)
        val request = SendDataRequest(data = Seq(chunkData), fileName = fileName)
        stub.sendDataToWorker(request)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to send data from worker ${workerID.get} to $dest: ${e.getMessage}")
    }
  }

  private def notifyMasterPartitionDone(): Unit = {
    assert(workerID.nonEmpty)
    try {
      val request = PhaseCompleteNotification(workerID = workerID.get)
      stub.partitionEndMsg(request)
      logger.info(s"Worker[${workerID.get}] has notified the master partitioning completed successfully.")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to receive partition acknowledgement: ${e.getMessage}")
        throw new RuntimeException("Ack error")
    }
  }

  private def notifyMasterShufflingDone(): Unit = {
    assert(workerID.nonEmpty)
    try {
      val request = PhaseCompleteNotification(workerID = workerID.get)
      stub.shufflingEndMsg(request)
      logger.info(s"Worker[${workerID.get}] has notified the master shuffling completed successfully.")
    } catch {
      case e: Exception =>
        logger.error(s"Error during shuffling: ${e.getMessage}")
        throw new RuntimeException("Shuffling Error")
    }
  }

  private def notifyMasterMergingDone(): Unit = {
    assert(workerID.nonEmpty)
    try {
      val request = PhaseCompleteNotification(workerID = workerID.get)
      stub.mergeEndMsg(request)
      logger.info(s"Worker[${workerID.get}] has notified the master merging completed successfully.")
    } catch {
      case e: Exception =>
        logger.error(s"Error during termination: ${e.getMessage}")
        throw new RuntimeException("Termination Error")
    }
  }

  private def deleteTempDirectories(): Unit = {
    (1 to totalWorkerCount.get).map(i => s"${outputDirectory}/$i").toList.
      foreach({s =>
        IOUtils.deleteFiles(IOUtils.getFilePathsFromDirectories(List(s)))
        Files.delete(Paths.get(s))
      })
  }
}


