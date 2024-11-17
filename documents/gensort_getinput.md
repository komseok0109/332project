# get input

ubuntu 진행

- gensort 100Byte 100000줄 생성

```scala
wget https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xvf gensort-linux-1.5.tar.gz 
cd 64
./gensort -a 100000 input
```

- scala 코드
    - 프로젝트 폴더 안의 input파일을 스트리밍으로 읽어들여
    - temp폴더에 100줄씩 끊어서 저장

```scala
import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object StreamingFileSplitter {
  def main(args: Array[String]): Unit = {
    val inputFilePath = "input"
    val tempDir = "temp"
    Files.createDirectories(Paths.get(tempDir)) // temp 디렉토리 생성

    splitFileWithStreaming(inputFilePath, tempDir)
  }

  def splitFileWithStreaming(inputFilePath: String, tempDir: String): Unit = {
    // ExecutorService와 ExecutionContext를 별도로 생성
    val executorService = Executors.newFixedThreadPool(4)
    implicit val ioEc: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

    try {
      val reader = new BufferedReader(new FileReader(inputFilePath))
      try {
        var line: String = null
        var linesBuffer = Vector.empty[String]
        var fileIndex = 1
        var futures = Vector.empty[Future[Unit]]

        while ({ line = reader.readLine(); line != null }) {
          linesBuffer = linesBuffer :+ line
          if (linesBuffer.size >= 100) {
            val fileName = s"$tempDir/output_$fileIndex.txt"
            val future = saveChunkAsync(fileName, linesBuffer)
            futures = futures :+ future
            linesBuffer = Vector.empty[String] // 버퍼 비우기
            fileIndex += 1
          }
        }

        // 남은 라인이 있는 경우 처리
        if (linesBuffer.nonEmpty) {
          val fileName = s"$tempDir/output_$fileIndex.txt"
          val future = saveChunkAsync(fileName, linesBuffer)
          futures = futures :+ future
        }

        // 모든 비동기 작업이 완료될 때까지 대기
        val aggregatedFuture = Future.sequence(futures)
        Await.result(aggregatedFuture, Duration.Inf)

      } finally {
        reader.close()
      }
    } catch {
      case ex: Exception =>
        println(s"파일 처리 중 에러 발생: ${ex.getMessage}")
    } finally {
      executorService.shutdown() // 여기서 ExecutorService를 종료합니다.
    }
  }

  def saveChunkAsync(fileName: String, lines: Seq[String])(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      val writer = new BufferedWriter(new FileWriter(fileName))
      try {
        lines.foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      } finally {
        writer.close()
      }
    }.recover {
      case ex: Exception =>
        println(s"$fileName 저장 실패: ${ex.getMessage}")
    }
  }
}

```
