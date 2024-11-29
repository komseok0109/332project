package functionality

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.io.{FileInputStream, BufferedInputStream}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString

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
            val inputStream = new BufferedInputStream(new FileInputStream(filePath))
            var bytesRead = 0
            val buffer = new Array[Byte](BYTES_PER_READ)
            var sampleCount = 0
            while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
              val byteString = ByteString.copyFrom(buffer.take(bytesRead))
              if (sampleCount < sampleNumOfEachFile) {
                sampleData.add(byteString.substring(0, 10))
                sampleCount += 1
              }
            }
            inputStream.close()
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
