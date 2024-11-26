import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer


class Merge() {
  def merge(inputPaths: List[String], outputPath: String): Unit = {
    val finalData = divideFiles(inputPaths)

    val writer = new PrintWriter(outputPath)
    try{
      finalData.foreach(writer.println)
    } finally {
      writer.close()
    }
    assert(isFileSorted(outputPath), s"Final merged file $outputPath is not sorted!")
  }

  def mergeTwoItr(data1: Iterator[String], data2: Iterator[String]): Iterator[String] = {
    val buffer = ListBuffer[String]()
    val it1 = data1.buffered
    val it2 = data2.buffered

    while (it1.hasNext && it2.hasNext) {
      if (it1.head <= it2.head) buffer += it1.next()
      else buffer += it2.next()
    }

    buffer ++= it1
    buffer ++= it2
    buffer.iterator
  }

  def divideFiles(filePathsList: List[String]): Iterator[String] = {
      if (filePathsList.size <= 1) {
        Source.fromFile(filePathsList.head).getLines()
      }
      else if (filePathsList.size == 2) {
        //비교할 파일이 2개라면 mergeTwoList로 합치기
        val data1 = Source.fromFile(filePathsList.head).getLines()
        val data2 = Source.fromFile(filePathsList(1)).getLines()
        mergeTwoItr(data1, data2)
      }
      else {
        val (firstHalf, secondHalf) = filePathsList.splitAt(filePathsList.size / 2)
        val firstFuture = Future { divideFiles(firstHalf) }
        val secondFuture = Future { divideFiles(secondHalf) }

        //Future를 Await 하고, 결과로 나온 값 두개를 mergeTwoLists()
        val data1 = Await.result(firstFuture, Duration.Inf)
        val data2 = Await.result(secondFuture, Duration.Inf)
        mergeTwoItr(data1, data2)
      }
  }
  private def isDataSorted(data: Iterator[String]): Boolean = {
    val keys = data.map(_.take(10))
    keys.sliding(2).forall {
      case Seq(prev, curr) => prev <= curr
      case _               => true
    }
  }

  def isFileSorted(fileName: String): Boolean = {
    try {
      // 파일을 읽어 List[String]으로 변환
      val source = Source.fromFile(fileName)
      val isSorted = isDataSorted(source.getLines())
      source.close()
      isSorted
    } catch {
      case ex: Exception =>
        println(s"Error reading the file: ${ex.getMessage}")
        false
    }
  }
}
