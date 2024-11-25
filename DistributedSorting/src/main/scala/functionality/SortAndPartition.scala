package functionality

import scala.io.Source
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import scala.collection.mutable
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging
import utils.IOUtils.getFilePathsFromDirectories

object SortAndPartition extends LazyLogging {
  def openFileAndProcessing(filePaths: List[String], key2Ranges: List[(Int, (String, String))],
                            outputDir: String, currentWorkerID: Int): Unit = {
    val numThreads = Runtime.getRuntime.availableProcessors()
    val executorService = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)
    try {
      val processingFutures = filePaths.map { filePath =>
        Future {
          processFile(filePath, key2Ranges, outputDir, currentWorkerID)
        }
      }
      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      executorService.shutdown()
    }
  }

  private def processFile(filePath: String, key2Ranges: List[(Int, (String, String))], outputDir: String, currentWorkerID: Int): Unit = {
    val chunkSize = 1000000
    try {
      val source = Source.fromFile(filePath)
      try {
        val lineIterator = source.getLines()
        val buffer = mutable.ArrayBuffer[String]()
        var chunkIndex = 0
        while (lineIterator.hasNext) {
          buffer.clear()
          while (buffer.size < chunkSize && lineIterator.hasNext) {
            buffer += lineIterator.next()
          }
          if (buffer.nonEmpty) {
            processChunk(buffer.toArray, key2Ranges, filePath, chunkIndex, outputDir, currentWorkerID)
            chunkIndex += 1
          }
        }
      } finally {
        source.close()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error during file($filePath) read: ${e.getMessage}")
    }
  }
  private def processChunk(chunk: Array[String], key2Ranges: List[(Int, (String, String))], filePath: String,
                   chunkIndex: Int, outputDir: String, currentWorkerID: Int): Unit = {
    val linesByRange = new mutable.HashMap[Int, mutable.ArrayBuffer[String]]()

    chunk.foreach { line =>
      if (line.nonEmpty) {
        val rangeKey = findRange(line, key2Ranges)
        rangeKey match {
          case Some(key) =>
            val lines = linesByRange.getOrElseUpdate(key._1, mutable.ArrayBuffer[String]())
            lines += line
          case None =>
            logger.warn(s"Line is not assigned to any range: $line")
        }
      }
    }

    linesByRange.foreach { case (key, lines) =>
      val sortedLines = lines.sorted
      val sanitizedFilePath = filePath.replaceAll("[^a-zA-Z0-9.-]", "_")
      val outputPath = s"$outputDir/$key/${sanitizedFilePath}_chunk_${chunkIndex}_Worker$currentWorkerID"
      val outputFile = new File(outputPath)

      val parentDir = outputFile.getParentFile
      if (!parentDir.exists()) {
        val dirCreated = parentDir.mkdirs()
        if (!dirCreated) {
          logger.warn(s"directory is not created: ${parentDir.getAbsolutePath}")
        }
      }

      try {
        val writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)))
        try {
          sortedLines.foreach(writer.println)
        } finally {
          writer.close()
        }
      } catch {
        case e: Exception =>
          logger.error(s"File($outputPath) write error: ${e.getMessage}")
      }
    }
  }


  private def findRange(line: String, keyRanges: List[(Int, (String, String))]): Option[(Int, String)] = {
    var low = 0
    var high = keyRanges.length - 1
    while (low <= high) {
      val mid = (low + high) / 2
      val (workerId, (startKey, endKey)) = keyRanges(mid)
      if (line >= startKey && line <= endKey) {
        val rangeLabel = s"${startKey}_${endKey}"
        return Some((workerId, rangeLabel))
      } else if (line < startKey) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    None
  }
}