package functionality

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream}
import scala.collection.mutable
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString
import com.google.protobuf.ByteString.unsignedLexicographicalComparator



object SortAndPartition extends LazyLogging {
  def openFileAndProcessing(filePaths: List[String], key2Ranges: List[(Int, (ByteString, ByteString))],
                            outputDir: String, currentWorkerID: Int, workerNum: Int, sortRequired: Boolean): Unit = {
    val numThreads = Runtime.getRuntime.availableProcessors()
    val executorService = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)
    try {
      val processingFutures = filePaths.map { filePath =>
        Future {
          processFile(filePath,
            key2Ranges.map(mapping => (mapping._1, (mapping._2._1, mapping._2._2))),
            outputDir, currentWorkerID, workerNum, sortRequired)
        }
      }

      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      executorService.shutdown()
    }
  }

  private def processFile(filePath: String, key2Ranges: List[(Int, (ByteString, ByteString))],
                          outputDir: String, currentWorkerID: Int, workerNum: Int, sortRequired: Boolean): Unit = {
    val chunkSize = 100000000
    try {
      val inputStream = new BufferedInputStream(new FileInputStream(filePath))
      try {
        val buffer = mutable.ArrayBuffer[ByteString]()
        var chunkIndex = 0
        val readBuffer = new Array[Byte](100)
        var bytesRead = inputStream.read(readBuffer)
        while (bytesRead != -1) {
          buffer.clear()
          while (buffer.size < chunkSize && bytesRead != -1) {
            val validBytes = ByteString.copyFrom(readBuffer.take(bytesRead))
            buffer += validBytes
            bytesRead = inputStream.read(readBuffer)
          }
          if (buffer.nonEmpty) {
            processChunk(buffer.toArray, key2Ranges, filePath, chunkIndex,
              outputDir, currentWorkerID, workerNum, sortRequired)
            chunkIndex += 1
          }
        }

      } finally {
        inputStream.close()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error during file($filePath) read: ${e.getMessage}")
    }
  }

  private def processChunk(chunk: Array[ByteString], key2Ranges: List[(Int, (ByteString, ByteString))],
                           filePath: String, chunkIndex: Int, outputDir: String,
                           currentWorkerID: Int, workerNum: Int, sortRequired: Boolean): Unit = {

    val linesByRange = new mutable.HashMap[Int, mutable.ArrayBuffer[ByteString]]()
    chunk.foreach { lineBytes =>
      if (!lineBytes.isEmpty) {
        val rangeKey = findRange(lineBytes, key2Ranges, workerNum)
        //logger.info(s"${rangeKey.get._1}, ${lineBytes.substring(0, 10)}")
        rangeKey match {
          case Some(key) =>
            val lines = linesByRange.getOrElseUpdate(key._1, mutable.ArrayBuffer[ByteString]())
            lines += lineBytes
          case None =>
            logger.warn(s"Line is not assigned to any range: $lineBytes")

        }
      }
    }
    linesByRange.foreach { case (key, lines) =>
      val sortedLines = {
        if (sortRequired) lines.sortWith((a, b) => unsignedLexicographicalComparator.compare(a ,b) < 0)
        else lines
      }
      val sanitizedFilePath = filePath.replaceAll("[^a-zA-Z0-9.-]", "_")
      val outputPath = {
        if (sortRequired) s"$outputDir/$key/${sanitizedFilePath}_chunk_${chunkIndex}_Worker${currentWorkerID}_to$key"
        else s"$outputDir/$key/${sanitizedFilePath}_chunk_${chunkIndex}_Worker${currentWorkerID}_Index$key"
      }
      val outputFile = new File(outputPath)

      val parentDir = outputFile.getParentFile
      if (!parentDir.exists()) {
        parentDir.mkdirs()
      }

      try {
        val outputStream = new FileOutputStream(outputPath)
        try {
          sortedLines.foreach({line => outputStream.write(line.toByteArray)})
        } finally {
          outputStream.close()
        }
      } catch {
        case e: Exception =>
          logger.error(s"File($outputPath) write error: ${e.getMessage}")
      }
    }
  }
  private def findRange(lineBytes: ByteString, keyRanges: List[(Int, (ByteString, ByteString))],
                        workerNum: Int): Option[(Int, ByteString)] = {
    if (workerNum == 1)
      Some((1, lineBytes))
    else {
      val key = lineBytes.substring(0, 10)
      keyRanges.sortBy(mapping => mapping._1).collectFirst {
        case (index, (start, end))
          if (index == workerNum && unsignedLexicographicalComparator().compare(key, start) >= 0)
            || unsignedLexicographicalComparator().compare(key, end) < 0 => (index, key)
      }
    }
  }
}
