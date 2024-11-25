package functionality

import scala.io.Source
import java.io.{File, PrintWriter, BufferedWriter, FileWriter}
import scala.collection.mutable
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging

object SortAndPartition extends LazyLogging {
  def openFileAndProcessing(filePaths: List[String], key2Ranges: List[(Int, (String, String))],
                            outputDir: String, currentWorkerID: Int): Unit = {
    val numThreads = Runtime.getRuntime.availableProcessors()
    val executorService = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)
    try {
      val processingFutures = processChunks(filePaths, key2Ranges, outputDir, currentWorkerID)
      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      executorService.shutdown()
    }
  }

  private def processChunks(filePaths: List[String], keyRanges: List[(Int, (String, String))], outputDir: String,
                            currentWorkerID: Int)(implicit ec: ExecutionContext): Seq[Future[Unit]] = {
    val chunkSizeInBytes = 10 * 1024 * 1024
    val buffer = mutable.ArrayBuffer[String]()
    var currentChunkSize = 0L
    var chunkIndex = 0

    val fileIterator = filePaths.iterator
    var currentSource: Option[Source] = None
    var lineIterator: Iterator[String] = Iterator.empty

    val processingFutures = mutable.Buffer[Future[Unit]]()

    try {
      while (fileIterator.hasNext || lineIterator.hasNext) {
        if (!lineIterator.hasNext && fileIterator.hasNext) {
          val filePath = fileIterator.next()
          try {
            currentSource.foreach(_.close())
            currentSource = Some(Source.fromFile(filePath))
            lineIterator = currentSource.get.getLines()
          } catch {
            case e: Exception =>
              logger.error(s"Error during file($filePath) read: ${e.getMessage}")
              lineIterator = Iterator.empty
          }
        }

        while (currentChunkSize < chunkSizeInBytes && lineIterator.hasNext) {
          val line = lineIterator.next()
          buffer += line
          currentChunkSize += line.getBytes("UTF-8").length + 1
        }

        if (currentChunkSize >= chunkSizeInBytes ||
          (!fileIterator.hasNext && !lineIterator.hasNext && buffer.nonEmpty)) {
          val chunkData = buffer.toArray
          val currentChunkIndex = chunkIndex
          val future = Future {
            processChunk(chunkData, keyRanges, currentChunkIndex, outputDir, currentWorkerID)
          }
          processingFutures += future
          chunkIndex += 1
          buffer.clear()
          currentChunkSize = 0L
        }
      }
    } finally {
      currentSource.foreach(_.close())
    }

    processingFutures.toSeq
  }

  private def processChunk(chunk: Array[String], keyRanges: List[(Int, (String, String))],
                           chunkIndex: Int, outputDir: String, currentWorkerID: Int): Unit = {
    val linesByRange = new mutable.HashMap[Int, mutable.ArrayBuffer[String]]()

    chunk.foreach { line =>
      if (line.nonEmpty) {
        val rangeInfo = findRange(line, keyRanges)
        rangeInfo match {
          case Some((workerId, rangeLabel)) =>
            val lines = linesByRange.getOrElseUpdate(workerId, mutable.ArrayBuffer[String]())
            lines += line
          case None =>
            logger.warn(s"Line is not assigned to any range: $line")
        }
      }
    }

    linesByRange.foreach { case (label, lines) =>
      val sortedLines = lines.sorted
      val outputPath = s"$outputDir/$label/chunk_$chunkIndex _worker_$currentWorkerID.txt"
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