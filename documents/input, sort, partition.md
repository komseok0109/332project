# sort, partition

build.sbt에 추가

```scala
libraryDependencies +=
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
```

- scala코드
    - input을 1000줄씩 받아온다
        - 1000줄을 정렬하고 범위마다 잘라 저장한다.
    - 위를 끝까지 반복한다.

```scala
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.util.concurrent.Executors

object ParallelChunkedSorterSplitter {
  def main(args: Array[String]): Unit = {
    val filePath = "input"
    //범위 설정(구현에선 받아와야함)
    val ranges = defaultRanges()

    // 출력 디렉토리 생성
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()

    // 고정 크기의 스레드 풀 생성 (예: 시스템의 코어 수에 따라 조절)
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessors))

    // 파일 소스 열기
    val source = Source.fromFile(filePath)

    try {
      // 파일을 1000줄씩 읽어오는 Iterator 생성
      val lineChunks = getLineChunks(source.getLines(), 1000)

      val processingFutures = mutable.Buffer[Future[Unit]]()

      // 각 청크를 순차적으로 가져와서 병렬로 처리
      while (lineChunks.hasNext) {
        val (chunkIndex, chunk) = lineChunks.next()
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

          // 각 범위별로 파일 저장 (chunkIndex 포함)
          linesByRange.foreach { case (label, lines) =>
            val outputPath = s"output/${label}_lines_chunk_$chunkIndex.txt"
            val writer = new PrintWriter(new File(outputPath))
            try {
              // 역순으로 저장 (리스트 앞에 추가했으므로)
              lines.reverse.foreach(writer.println)
            } finally {
              writer.close()
            }
          }

          println(s"Chunk $chunkIndex 처리 완료.")
        }
        processingFutures += future
      }

      // 모든 청크 처리 완료 대기
      Await.result(Future.sequence(processingFutures), Duration.Inf)
    } finally {
      // 파일 소스 닫기
      source.close()
      ec.shutdown()
    }
  }

  // 파일을 지정한 크기의 청크로 Iterator를 반환하는 함수
  def getLineChunks(linesIterator: Iterator[String], chunkSize: Int): Iterator[(Int, List[String])] = {
    var chunkIndex = 0

    new Iterator[(Int, List[String])] {
      def hasNext: Boolean = linesIterator.hasNext
      def next(): (Int, List[String]) = {
        val chunk = linesIterator.take(chunkSize).toList
        val currentIndex = chunkIndex
        chunkIndex += 1
        (currentIndex, chunk)
      }
    }
  }

  //범위 예시
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

  // 동기화된 PrintWriter 클래스 정의
  class SynchronizedPrintWriter(file: File) {
    private val writer = new PrintWriter(file)
    def println(line: String): Unit = this.synchronized {
      writer.println(line)
    }
    def close(): Unit = writer.close()
  }
}

```