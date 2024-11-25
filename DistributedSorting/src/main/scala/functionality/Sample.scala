package functionality

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.Random
import scala.concurrent.duration._
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.LazyLogging

object Sample extends LazyLogging {
  private val NUM_MAX_SAMPLE = 100000

  def sampleFile(filesPathList: List[String]): List[String] = {
    val sampleData = new ConcurrentLinkedQueue[String]()

    val numGroups = math.max(1, filesPathList.length / 4)
    val sampleNumOfEachFile = NUM_MAX_SAMPLE / filesPathList.length
    val futures = filesPathList.grouped(numGroups).map { group =>
      Future {
        group.foreach { filePath =>
          try {
            val source = Source.fromFile(filePath)
            val linesList = source.getLines().map(_.splitAt(10)).toList
            source.close()
            val keyArray = Random.shuffle(linesList.map(_._1))
            val sampleNumOfFile = Math.min(sampleNumOfEachFile, keyArray.length)
            keyArray.take(sampleNumOfFile).foreach(sampleData.add)
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