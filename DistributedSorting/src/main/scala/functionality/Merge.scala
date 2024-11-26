import java.io._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._
import scala.collection._
import scala.collection.mutable.ListBuffer


class Merge {
  val threadsNum = 4


  def mergeSort(inputPath: String, outputPath: String, workerNum: Int): List[String] = {
    // Step 1: Read the input file and determine key ranges
    val inputDataList = readFiles(inputPath, workerNum)

    val keys = inputDataList.map(_.take(10))
    //key 들 list 얻은 후, 정렬
    val sortedKeys = keys.sorted

    //정렬한 key로 key range 분리
    val ranges = getRange(sortedKeys)

    // Step 2: Split input data into key-based regions
    //thread 개수만큼 빈 list (listbuffer) 생성
    val partitionedData = splitData(inputDataList, ranges)

    // Step 3: Sort each region concurrently
    val sortAndWriteFutures = partitionedData.zipWithIndex.map { case (data, i) =>
      Future {
        //나눠진 부분 각각 정렬
        val sortedData = data.sorted

        val writeFilePath = outputPath + s"-fromThread.$i.txt"
        //outputPath+"fromThread3.txt" 같은 형식으로 저장
        val writer = new PrintWriter(writeFilePath)
        sortedData.foreach(line => writer.println(line))
        writer.close()
        writeFilePath
      }
    }

    // Step 4: Wait for all sorting tasks to complete
    val outputFilesList = Await.result(Future.sequence(sortAndWriteFutures), Duration.Inf)
    mergeAllFiles(outputFilesList,outputPath+".merged.txt")
    outputFilesList
  }

  private def readFiles(inputPath: String, workerNum: Int): List[String] = {
    val inputDataList = (0 until workerNum).flatMap { i =>
      //파일 이름이 inputPath+".3.txt" 같은 형식으로 되어있다고 가정
      //C://somepath/somename-worker.1.txt : somepath 폴더 안의 somename-worker.1.txt
      val fileName = inputPath + s".$i.txt"
      try {
        val source = Source.fromFile(fileName)
        source.getLines().toList
      } catch {
        case ex: FileNotFoundException =>
          println(s"File not found: $fileName. Skipping...")
          Iterator.empty // 빈 Iterator를 반환하여 스킵
        case ex: IOException =>
          println(s"Error while reading file: $fileName. ${ex.getMessage}. Skipping...")
          Iterator.empty // 빈 Iterator를 반환하여 스킵
        case ex: Exception =>
          println(s"Unexpected error while reading files: ${ex.getMessage}")
          Iterator.empty
      }
    }.toList

    inputDataList
  }

  private def getRange(keys: List[String]): List[String] = {
    val ranges = {
      if (keys.nonEmpty) {
        (0 until threadsNum).map(i => keys((i * keys.size) / threadsNum)).toList :+ keys.last
      }
      else {
        List.empty[String]
      }
    }
    //println("Ranges: [" + ranges.mkString("],[")+"]")
    ranges
  }

  private def splitData(inputDataLines: List[String], ranges: List[String]): List[ListBuffer[String]] = {
    val partitionedData = Array.fill(threadsNum)(collection.mutable.ListBuffer.empty[String])

    //input data의 모든 라인에 대해
    inputDataLines.foreach { line =>
      //key 획득
      val key = line.take(10)
      //key가 어느 영역인지 index 획득
      try {
        //여기서 range를 (0,25,50,75,100)이라고 하면, key 30은 index가 2로 나옴. (25,50) 범위가 2번째
        val regionIndex = ranges.indexWhere(key < _) match {
          case -1 => threadsNum - 1
          //index가 0,1,2,3 이 아니라 1,2,3,4로 나와서 1 빼줌
          case idx => idx-1
        }
        //해당 index의 listbuffer에 line 추가
        partitionedData(regionIndex) += line
      } catch {
        case ex: IndexOutOfBoundsException =>
          println(s"Error: Index out of bounds while processing key '$key'.")
        case ex: Exception =>
          println(s"Unexpected error while processing key '$key': ${ex.getMessage}")
      }
    }

    partitionedData.toList
  }

  def mergeAllFiles(filesList : List[String], outputPath: String): Unit={
    val writer = new PrintWriter(new File(outputPath))
    try {
      // 각 파일을 순회하며 내용 읽기
      for (file <- filesList) {
        val source = Source.fromFile(file)
        try {
          val inputDataLines = source.getLines().toList

          //val isSorted = isDataSorted(inputDataLines)
          //println(s"Data is sorted: $isSorted")

          //파일 내용 쓰기
          inputDataLines.foreach { line =>
            writer.println(line)
          }
        } finally {
          source.close() // 개별 파일 닫기
        }
      }
    } catch {
      case ex: IOException =>
        println(s"Error while merging files: ${ex.getMessage}")
    } finally {
      writer.close() // 출력 파일 닫기
    }
  }

  private def isDataSorted(inputDataList: List[String]): Boolean = {
    val keys = inputDataList.map(_.take(10))
    keys.zip(keys.tail).forall { case (prev, curr) => prev <= curr }
  }

  def isFileSorted(fileName: String): Boolean = {
    try {
      // 파일을 읽어 List[String]으로 변환
      val source = Source.fromFile(fileName)
      val lines = source.getLines().toList
      source.close()
      // 읽은 데이터를 isDataSorted 함수에 전달하여 정렬 여부 확인
      isDataSorted(lines)
    } catch {
      case ex: Exception =>
        println(s"Error reading the file: ${ex.getMessage}")
        false
    }
  }
}
