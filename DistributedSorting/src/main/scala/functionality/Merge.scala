import java.io._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._

class Merge {

  def mergeSort(inputPath: String, outputPath: String, totalWorkerNum: Int): Unit = {
    // Step 1: Read the input file and determine key ranges
    val linesList = Source.fromFile(inputPath).getLines().toList
    val keys = linesList.map(_.take(10))
    //key 들 list 얻은 후, 정렬
    val sortedKeys = keys.sorted

    //val rangeSize = sortedKeys.size / totalWorkerNum

    //정렬한 key로 key range 분리
    val ranges = (0 until totalWorkerNum).map(i => sortedKeys((i * sortedKeys.size) / totalWorkerNum)).toList :+ sortedKeys.last

    // Step 2: Split input data into key-based regions
    //thread 개수만큼 빈 list (listbuffer) 생성
    val splitData = Array.fill(totalWorkerNum)(collection.mutable.ListBuffer.empty[String])
    
    //input data의 모든 라인에 대해
    linesList.foreach { line =>
      //key 획득
      val key = line.take(10)
      
      //key가 어느 영역인지 index 획득
      val regionIndex = ranges.indexWhere(key < _) match {
        case -1 => totalWorkerNum - 1
        case idx => idx
      }
      //해당 index의 listbuffer에 line 추가
      splitData(regionIndex) += line
    }

    // Step 3: Sort each region concurrently
    val sortedFutures = splitData.zipWithIndex.map { case (data, i) =>
      Future {
        val sortedData = data.sorted
        val writer = new PrintWriter(outputPath + s"fromMachine.$i.txt")
        sortedData.foreach(line => writer.println(line))
        writer.close()
        sortedData
      }
    }

    // Step 4: Wait for all sorting tasks to complete
    val sortedResults = Await.result(Future.sequence(sortedFutures), Duration.Inf)

    // Step 5: Merge all sorted results
    val mergedSortedLines = mergeSortedLists(sortedResults)

    // Step 6: Write the final merged sorted data to the output file
    val writer = new PrintWriter(outputPath + "Result.txt")
    mergedSortedLines.foreach(line => writer.println(line))
    writer.close()
  }

  // Helper function to merge sorted lists
  private def mergeSortedLists(sortedLists: List[List[String]]): List[String] = {
    val pq = collection.mutable.PriorityQueue.empty[(String, Int)](
      Ordering.by[(String, Int), String](_._1).reverse
    )

    // Initialize priority queue with the first element of each sorted list
    sortedLists.zipWithIndex.foreach { case (list, idx) =>
      if (list.nonEmpty) pq.enqueue((list.head, idx))
    }

    val merged = collection.mutable.ListBuffer.empty[String]
    val iterators = sortedLists.map(_.iterator)

    // Merge sorted lists using the priority queue
    while (pq.nonEmpty) {
      val (minValue, idx) = pq.dequeue()
      merged += minValue

      if (iterators(idx).hasNext) {
        pq.enqueue((iterators(idx).next(), idx))
      }
    }
    merged.toList
  }
}
