package utils

import utils.IOUtils.getFilePathsFromDirectories
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._

object InputGenerator {
  def main(args: Array[String]): Unit = {
    val directories = List("DistributedSorting/input1", "DistributedSorting/input2", "DistributedSorting/input3")
    val filePaths = getFilePathsFromDirectories(directories)
    getInput(filePaths)
  }

  def getInput(filePathList: List[String]): Unit = {
    // 고정 크기의 스레드 풀 생성
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessors))

    try {
      // 파일별 Iterator 처리
      filePathList.zipWithIndex.foreach { case (oneFile, fileIndex) =>
        val source = Source.fromFile(oneFile)
        try {
          // 파일에서 320000줄씩 청크 Iterator 생성
          val lineChunks = getLineChunks(source.getLines(), 320000)

          // 청크 처리
          lineChunks.zipWithIndex.foreach { case (chunk, chunkIndex) =>
            // 원래 파일이 있는 디렉토리 기반으로 출력 파일 경로 생성
            val originalDir = new File(oneFile).getParent
            val chunkFilePath = s"$originalDir/file${fileIndex}_$chunkIndex.txt"

            // 디렉토리 생성 확인
            val chunkFile = new File(chunkFilePath)
            if (!chunkFile.getParentFile.exists()) chunkFile.getParentFile.mkdirs()

            // 청크 데이터를 비동기적으로 저장
            val future = Future {
              val writer = new PrintWriter(chunkFile)
              try {
                chunk.foreach(writer.println)
              } finally {
                writer.close()
              }
              println(s"File $fileIndex, Chunk $chunkIndex 저장 완료: $chunkFilePath")
            }

            // 처리 완료 대기
            Await.result(future, Duration.Inf)
          }
        } finally {
          // 파일 소스 닫기
          source.close()
        }
      }
    } finally {
      // 스레드 풀 종료
      ec.shutdown()
    }
  }

  // 파일을 지정한 크기의 청크로 Iterator를 반환하는 함수
  def getLineChunks(linesIterator: Iterator[String], chunkSize: Int): Iterator[List[String]] = {
    new Iterator[List[String]] {
      def hasNext: Boolean = linesIterator.hasNext
      def next(): List[String] = linesIterator.take(chunkSize).toList
    }
  }
}
