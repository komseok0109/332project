package functionality

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.io._
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString
import scala.util.Random

object Sample extends LazyLogging {
  private val NUM_MAX_SAMPLE = 10000
  private val BYTES_PER_READ = 100

  def sampleFile(filesPathList: List[String]): List[ByteString] = {
    val sampleData = new ConcurrentLinkedQueue[ByteString]()

    val numGroups = math.max(1, filesPathList.length / 4)
    val sampleNumOfEachFile = NUM_MAX_SAMPLE / filesPathList.length
    val futures = filesPathList.grouped(numGroups).map { group =>
      Future {
        group.foreach { filePath =>
          try {
            val file = new RandomAccessFile(filePath, "r")
            val fileLength = file.length()
            val numBlocks = (fileLength / BYTES_PER_READ).toInt
            val possibleBlocks = (0 until numBlocks).toList

            val sampledBlocks = Random.shuffle(possibleBlocks).take(sampleNumOfEachFile)
            sampledBlocks.foreach { blockIndex =>
              val position = blockIndex * BYTES_PER_READ
              file.seek(position)
              val buffer = new Array[Byte](BYTES_PER_READ)
              val bytesRead = file.read(buffer, 0, BYTES_PER_READ)
              if (bytesRead > 0) {
                val byteString = ByteString.copyFrom(buffer.take(bytesRead))
                sampleData.add(byteString.substring(0, 10))
              }
            }
            file.close()
          } catch {
            case ex: Exception => logger.error(s"Error processing file $filePath: ${ex.getMessage}")
          }
        }
      }
    }
    Await.result(Future.sequence(futures.toList), Duration.Inf)
    sampleData.asScala.toList
  }
}
