package functionality

import java.io.{BufferedInputStream, BufferedWriter, File, FileInputStream, FileWriter, PrintWriter}
import scala.collection.mutable
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging

object SortAndPartition extends LazyLogging {
  def openFileAndProcessing(filePaths: List[String], key2Ranges: List[(Int, (Array[Byte], Array[Byte]))],
                            outputDir: String, currentWorkerID: Int, workerNum: Int): Unit = {
    val numThreads = Runtime.getRuntime.availableProcessors()
    val executorService = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)
    try {
      val processingFutures = filePaths.map { filePath =>
        Future {
          logger.info(s"Processing started for file: $filePath")
          processFile(filePath, key2Ranges, outputDir, currentWorkerID, workerNum)
          logger.info(s"Processing finished for file: $filePath")
        }
      }

      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      executorService.shutdown()
    }
  }

  private def processFile(filePath: String, key2Ranges: List[(Int, (Array[Byte], Array[Byte]))],
                          outputDir: String, currentWorkerID: Int, workerNum: Int): Unit = {
    val chunkSize = 1000000
    try {
      val inputStream = new BufferedInputStream(new FileInputStream(filePath))
      try {
        val buffer = mutable.ArrayBuffer[Array[Byte]]()
        var chunkIndex = 0
        val readBuffer = new Array[Byte](100)
        var bytesRead = inputStream.read(readBuffer)

        while (bytesRead != -1) {
          buffer.clear()
          while (buffer.size < chunkSize && bytesRead != -1) {
            val validBytes = readBuffer.take(bytesRead) // 읽은 데이터만 추가
            buffer += validBytes
            bytesRead = inputStream.read(readBuffer)
          }
          if (buffer.nonEmpty) {
            processChunk(buffer.toArray, key2Ranges, filePath, chunkIndex, outputDir, currentWorkerID, workerNum)
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

  private def processChunk(chunk: Array[Array[Byte]], key2Ranges: List[(Int, (Array[Byte], Array[Byte]))], filePath: String,
                           chunkIndex: Int, outputDir: String, currentWorkerID: Int, workerNum: Int): Unit = {
    val linesByRange = new mutable.HashMap[Int, mutable.ArrayBuffer[Array[Byte]]]()

    chunk.foreach { lineBytes =>
      if (lineBytes.nonEmpty) {
        val rangeKey = findRange(lineBytes, key2Ranges, workerNum)
        rangeKey match {
          case Some(key) =>
            val lines = linesByRange.getOrElseUpdate(key._1, mutable.ArrayBuffer[Array[Byte]]())
            lines += lineBytes
          case None =>
            logger.warn(s"Line is not assigned to any range: ${new String(lineBytes, "UTF-8")}")
        }
      }
    }

    linesByRange.foreach { case (key, lines) =>
      val sortedLines = lines.sortBy(line => new String(line, "UTF-8"))
      val sanitizedFilePath = filePath.replaceAll("[^a-zA-Z0-9.-]", "_")
      val outputPath = s"$outputDir/$key/${sanitizedFilePath}_chunk_${chunkIndex}_Worker${currentWorkerID}_to$key"
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
          sortedLines.foreach(line => writer.println(new String(line, "UTF-8")))
        } finally {
          writer.close()
        }
      } catch {
        case e: Exception =>
          logger.error(s"File($outputPath) write error: ${e.getMessage}")
      }
    }
  }

  private def findRange(lineBytes: Array[Byte], keyRanges: List[(Int, (Array[Byte], Array[Byte]))], workerNum: Int): Option[(Int, String)] = {
    val key = lineBytes.take(10)
    keyRanges.sortBy(mapping => mapping._1).collectFirst {
      case (index, (start, end))
        if (index == workerNum && compareBytes(key, start) >= 0) || compareBytes(key, end) <= 0 => (index, new String(key, "UTF-8"))
    }
  }

  private def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    val length = math.min(a.length, b.length)
    for (i <- 0 until length) {
      val cmp = java.lang.Byte.compare(a(i), b(i))
      if (cmp != 0) return cmp
    }
    a.length - b.length
  }
}
