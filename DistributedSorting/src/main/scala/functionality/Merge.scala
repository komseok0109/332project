package functionality

import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString
import com.google.protobuf.ByteString.unsignedLexicographicalComparator
import utils._

import java.io._
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.Duration

object Merge extends LazyLogging{
  private val PARTITION_NUM = 4

  def merge(outputDir: String, currentWorkerID: Int): Unit ={
    val executorService = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)
    try {
      val futureList = (1 to PARTITION_NUM).map{ i =>
        Future {
          val tempPath = outputDir + s"/${i}"
          val tempFiles = IOUtils.getFilePathsFromDirectories(List(tempPath))
          mergeSortedFiles(tempFiles, outputDir + s"/partition${currentWorkerID * 10 + i}")
          IOUtils.deleteFiles(tempFiles)
          Files.delete(Paths.get(tempPath))
        }
      }
      Await.result(Future.sequence(futureList), Duration.Inf)
    } catch {
      case e: Exception => logger.error(s"Error during merge: ${e.getMessage}")
    } finally {
      executorService.shutdown()
    }
  }

  def getRangeMapping(filePaths: List[String],
                              myRange: (ByteString, ByteString)): List[(Int, (ByteString, ByteString))] = {
    val sortedSamples = Sample.sampleFile(filePaths)
                        .sortWith((a,b) =>unsignedLexicographicalComparator().compare(a, b) < 0)
    val rangeStep = sortedSamples.length / PARTITION_NUM
    (for {i <- 0 until PARTITION_NUM
      startKey = if (i == 0) myRange._1 else sortedSamples(i * rangeStep)
      endKey = {
        if (i == PARTITION_NUM - 1) myRange._2
        else sortedSamples((i + 1) * rangeStep)
      }
    } yield (i + 1, (startKey, endKey))).toList
  }

  private def mergeSortedFiles(inputFiles: List[String], outputFile: String): Unit = {
    implicit val ordering: Ordering[(ByteString, Int)] = Ordering.fromLessThan { (a, b) =>
      unsignedLexicographicalComparator().compare(a._1.substring(0, 10), b._1.substring(0, 10)) > 0
    }
    val pq = new mutable.PriorityQueue[(ByteString, Int)]()(ordering)
    val readers = inputFiles.map(file => new BufferedInputStream(new FileInputStream(file)))

    readers.zipWithIndex.foreach { case (reader, index) =>
      val readBuffer = new Array[Byte](100)
      val bytesRead = reader.read(readBuffer)
      val validBytes = ByteString.copyFrom(readBuffer.take(bytesRead))
      pq.enqueue((validBytes, index))
    }
    val writer = new BufferedOutputStream(new FileOutputStream(outputFile))
    while (pq.nonEmpty) {
      val (data, fileIndex) = pq.dequeue()
      val reader = readers(fileIndex)
      writer.write(data.toByteArray)
      val readBuffer = new Array[Byte](100)
      val bytesRead = reader.read(readBuffer)
      if (bytesRead > 0) {
        val validBytes = ByteString.copyFrom(readBuffer.take(bytesRead))
        pq.enqueue((validBytes, fileIndex))
      }
    }
    readers.foreach(_.close())
    writer.close()
  }
}
