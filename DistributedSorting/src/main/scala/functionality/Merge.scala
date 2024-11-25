import java.io._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._


class Merge {
  val threadsNum = 4

  def mergeSort(inputPath: String, outputPath: String): List[String] = {
    // Step 1: Read the input file and determine key ranges
    //파일 이름이 inputPath+"worker.3.txt" 같은 형식으로 되어있다고 가정
    val linesList = (0 until threadsNum).flatMap { i =>
      val fileName = inputPath + s"worker.$i.txt"
      Source.fromFile(fileName).getLines()
    }.toList
    val keys = linesList.map(_.take(10))
    //key 들 list 얻은 후, 정렬
    val sortedKeys = keys.sorted

    //정렬한 key로 key range 분리
    val ranges = (0 until threadsNum).map(i => sortedKeys((i * sortedKeys.size) / threadsNum)).toList :+ sortedKeys.last

    // Step 2: Split input data into key-based regions
    //thread 개수만큼 빈 list (listbuffer) 생성
    val splitData = Array.fill(threadsNum)(collection.mutable.ListBuffer.empty[String])

    //input data의 모든 라인에 대해
    linesList.foreach { line =>
      //key 획득
      val key = line.take(10)

      //key가 어느 영역인지 index 획득
      val regionIndex = ranges.indexWhere(key < _) match {
        case -1 => threadsNum - 1
        case idx => idx
      }
      //해당 index의 listbuffer에 line 추가
      splitData(regionIndex) += line
    }

    // Step 3: Sort each region concurrently
    val sortedFutures = splitData.zipWithIndex.map { case (data, i) =>
      Future {
        //나눠진 부분 각각 정렬
        val sortedData = data.sorted

        val writtenFilePath = outputPath + s"fromThread.$i.txt"
        //outputPath+"fromThread3.txt" 같은 형식으로 저장
        val writer = new PrintWriter(writtenFilePath)
        sortedData.foreach(line => writer.println(line))
        writer.close()
        writtenFilePath
      }
    }.toList

    // Step 4: Wait for all sorting tasks to complete
    val writtenFiles = Await.result(Future.sequence(sortedFutures), Duration.Inf)

    writtenFiles
  }
}
