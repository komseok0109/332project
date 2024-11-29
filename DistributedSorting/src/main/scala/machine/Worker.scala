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
import scala.io.Source
import java.util.concurrent.Executors
import com.google.protobuf.ByteString

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
  private val registeredWorkersIP: TrieMap[Int, ByteString] = TrieMap()
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
      SortAndPartition.openFileAndProcessing(filePaths, ID2Ranges.toList, outputDirectory,
        workerID.get, totalWorkerCount.get)
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
    } catch {
      case e: Exception =>
        logger.error(s"Shuffling Error: ${e.getMessage}")
        shutDownChannel()
        deleteTempDirectories()
        System.exit(1)
    }
    try {
      deleteTempDirectories()
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

  private def registerToMaster(): Unit = ???

  private def sendSamplesToMaster(): Unit = ???

  private def shuffle(): Unit = ???

  private def shuffleData(stub: ShufflingMessageGrpc.ShufflingMessageBlockingStub, source: Source,
                          dest: Int, fileName: String): Unit = ???

  private def notifyMasterPartitionDone(): Unit = ???

  private def notifyMasterShufflingDone(): Unit = ???

  private def notifyMasterMergingDone(): Unit = ???

  private def deleteTempDirectories(): Unit = {
    (1 to totalWorkerCount.get).map(i => s"${outputDirectory}/$i").toList.
      foreach({s =>
        IOUtils.deleteFiles(IOUtils.getFilePathsFromDirectories(List(s)))
        Files.delete(Paths.get(s))
      })
  }
}


