package functionality

import utils.IOUtils.getFilePathsFromDirectories
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.collection.concurrent._

object SortAndPartition{
  def sortAndPartition(filePaths: List[String], keyRanges: TrieMap[Int, (String, String)]): Unit = {

    // 고정 크기의 스레드 풀 생성
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessors))

    try {
      val processingFutures = mutable.Buffer[Future[Unit]]()

      //100MB단위로 끊어서 처리
      val lineIterator = filePaths.iterator.flatMap(filePath => Source.fromFile(filePath).getLines())
      processChunks(lineIterator, 1000000, keyRanges, processingFutures)

      // 모든 작업이 완료될 때까지 대기
      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      ec.shutdown()
    }
  }

  def processChunks(
                     lineIterator: Iterator[String],
                     chunkSize: Int,
                     ranges: TrieMap[Int, (String, String)],
                     processingFutures: mutable.Buffer[Future[Unit]]
                   )(implicit ec: ExecutionContext): Unit = {
    var chunkIndex = 0
    val buffer = mutable.ListBuffer[String]()

    while (lineIterator.hasNext) {
      // 필요한 만큼 줄을 읽음
      while (buffer.size < chunkSize && lineIterator.hasNext) {
        buffer += lineIterator.next()
      }

      if (buffer.nonEmpty) {
        val chunk = buffer.toList
        buffer.clear() // 버퍼 비우기

        val currentChunkIndex = chunkIndex // 현재 chunkIndex를 캡처
        chunkIndex += 1

        val future = Future {
          // 청크 정렬
          val sortedChunk = chunk.sorted

          // 범위별로 라인 분류
          val linesByRange = mutable.Map[String, List[String]]().withDefaultValue(List())

          sortedChunk.foreach { line =>
            if (line.nonEmpty) {
              val firstChar = line.head
              ranges.foreach { case (label, predicate) =>
                if (predicate(firstChar)) {
                  linesByRange(label) = line :: linesByRange(label)
                }
              }
            }
          }

          // 범위별로 파일 저장
          linesByRange.foreach { case (label, lines) =>
            val outputPath = s"DistributedSorting/output/${label}_lines_chunk_$currentChunkIndex.txt"
            val outputFile = new File(outputPath)

            // 디렉토리 확인 및 생성
            val parentDir = outputFile.getParentFile
            if (!parentDir.exists()) {
              parentDir.mkdirs() // 디렉토리가 없으면 생성
            }

            val writer = new PrintWriter(outputFile)
            try {
              // 역순으로 저장 (리스트 앞에 추가했으므로)
              lines.reverse.foreach(writer.println)
            } finally {
              writer.close()
            }
          }

          println(s"Chunk $currentChunkIndex 처리 완료.")
        }
        processingFutures += future
      }
    }
  }

  // 범위 정의
  def defaultRanges(): List[(String, Char => Boolean)] = {
    List(
      ("Special Characters", (c: Char) => !c.isLetterOrDigit),
      ("Numbers 0-4", (c: Char) => c.isDigit && c >= '0' && c <= '4'),
      ("Numbers 5-9", (c: Char) => c.isDigit && c >= '5' && c <= '9'),
      ("Alphabets a-f", (c: Char) => c.isLetter && c.toLower >= 'a' && c.toLower <= 'f'),
      ("Alphabets g-p", (c: Char) => c.isLetter && c.toLower >= 'g' && c.toLower <= 'p'),
      ("Alphabets q-z", (c: Char) => c.isLetter && c.toLower >= 'q' && c.toLower <= 'z')
    )
  }

}