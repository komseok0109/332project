package functionality

import utils.IOUtils.getFilePathsFromDirectories

import scala.io.Source
import java.io.{File, PrintWriter, BufferedWriter, FileWriter}
import scala.collection.mutable
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._

object ParallelChunkedSorterSplitter {
  def main(args: Array[String]): Unit = {
    val directories = List("input1", "input2", "input3")
    val filePaths = getFilePathsFromDirectories(directories)
    val keyRanges = List(
      (1, ("          ", "cccccccccc")),
      (2, ("cccccccccd", "ffffffffff")),
      (3, ("fffffffffg", "~~~~~~~~~~"))
    )
    openFileAndProcessing(filePaths, keyRanges, "output", 1)
  }

  def openFileAndProcessing(filePaths: List[String], keyRanges: List[(Int, (String, String))], outputDir: String, currentWorkerID: Int): Unit = {
    // 고정 크기의 스레드 풀 생성
    val numThreads = Runtime.getRuntime.availableProcessors()
    val executorService = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

    try {
      // 청크 단위로 처리
      val processingFutures = processChunks(filePaths, keyRanges, outputDir, currentWorkerID)

      // 모든 작업이 완료될 때까지 대기
      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      // ExecutorService 종료
      executorService.shutdown()
    }
  }

  def processChunks(filePaths: List[String], keyRanges: List[(Int, (String, String))], outputDir: String, currentWorkerID: Int)(implicit ec: ExecutionContext): Seq[Future[Unit]] = {
    val chunkSizeInBytes = 100 * 1024 * 1024 // 100MB
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
          // 다음 파일 열기
          val filePath = fileIterator.next()
          try {
            currentSource.foreach(_.close())
            currentSource = Some(Source.fromFile(filePath))
            lineIterator = currentSource.get.getLines()
          } catch {
            case e: Exception =>
              println(s"파일을 읽는 중 오류 발생: $filePath, 오류: ${e.getMessage}")
              lineIterator = Iterator.empty
          }
        }

        while (currentChunkSize < chunkSizeInBytes && lineIterator.hasNext) {
          val line = lineIterator.next()
          buffer += line
          currentChunkSize += line.getBytes("UTF-8").length + 1 // 개행 문자 고려
        }

        if (currentChunkSize >= chunkSizeInBytes || (!fileIterator.hasNext && !lineIterator.hasNext && buffer.nonEmpty)) {
          // 청크 처리
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
      // 파일 닫기
      currentSource.foreach(_.close())
    }

    processingFutures.toSeq // 불변 Seq로 변환하여 반환
  }

  def processChunk(chunk: Array[String], keyRanges: List[(Int, (String, String))], chunkIndex: Int, outputDir: String, currentWorkerID: Int): Unit = {
    // 범위별 라인 저장을 위한 맵
    val linesByRange = new mutable.HashMap[String, mutable.ArrayBuffer[String]]()

    // 라인을 범위에 할당
    chunk.foreach { line =>
      if (line.nonEmpty) {
        val rangeInfo = findRange(line, keyRanges)
        rangeInfo match {
          case Some((workerId, rangeLabel)) =>
            val lines = linesByRange.getOrElseUpdate(rangeLabel, mutable.ArrayBuffer[String]())
            lines += line
          case None =>
            println(s"라인이 어떤 범위에도 할당되지 않았습니다: $line")
        }
      }
    }

    // 범위별로 라인 정렬 및 파일 쓰기
    linesByRange.foreach { case (label, lines) =>
      val sortedLines = lines.sorted
      val outputPath = s"$outputDir/$label/chunk_$chunkIndex _worker_$currentWorkerID.txt"
      val outputFile = new File(outputPath)

      // 부모 디렉토리 생성 확인
      val parentDir = outputFile.getParentFile
      if (!parentDir.exists()) {
        val dirCreated = parentDir.mkdirs()
        if (!dirCreated) {
          println(s"디렉토리를 생성하지 못했습니다: ${parentDir.getAbsolutePath}")
        }
      }

      try {
        val writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true))) // append 모드
        try {
          sortedLines.foreach(writer.println)
        } finally {
          writer.close()
        }
      } catch {
        case e: Exception =>
          println(s"파일 쓰기 중 오류 발생: $outputPath, 오류: ${e.getMessage}")
      }
    }

    println(s"청크 $chunkIndex 처리 완료.")
  }

  def findRange(line: String, keyRanges: List[(Int, (String, String))]): Option[(Int, String)] = {
    // 이진 검색을 사용하여 범위 찾기
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
