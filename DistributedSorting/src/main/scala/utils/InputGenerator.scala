package utils

import utils.IOUtils.getFilePathsFromDirectories
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._

object InputGenerator {
  def main(args: Array[String]): Unit = {
    val filePaths = getFilePathsFromDirectories(args.toList)
    getInput(filePaths)
  }

  def getInput(filePathList: List[String]): Unit = {
    val outputDir = new File("DistributedSorting/output")
    if (!outputDir.exists()) outputDir.mkdirs()

    val availableProcessors = Runtime.getRuntime.availableProcessors()
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessors))

    try {
      filePathList.zipWithIndex.foreach { case (oneFile, fileIndex) =>
        val source = Source.fromFile(oneFile)
        try {
          // 파일에서 320000줄씩 청크 Iterator 생성
          val lineChunks = getLineChunks(source.getLines(), 320000)

          // 청크 처리
          lineChunks.zipWithIndex.foreach { case (chunk, chunkIndex) =>
            val chunkFilePath = s"DistributedSorting/output/file${fileIndex}_chunk$chunkIndex.txt"

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
