package utils

import utils.IOUtils.{deleteFiles, getFilePathsFromDirectories}

import scala.io.Source
import java.io.{File, PrintWriter}
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._

object InputGenerator {
  def main(args: Array[String]): Unit = {
    val inputGenerator = new InputGenerator(args.toList)
    inputGenerator.generateInputs()
  }
}

class InputGenerator(inputDirectories: List[String]) {
  private val CHUNK_SIZE = 320000

  def generateInputs(): Unit = {
    inputDirectories.foreach(dir => {
      val filePaths = getFilePathsFromDirectories(List(dir))
      generateInput(dir, filePaths)
      deleteFiles(filePaths)
    })
  }

<<<<<<< HEAD
  private def generateInput(directory: String, filePathList: List[String]): Unit = {
    val outputDir = new File(directory)
    if (!outputDir.exists()) outputDir.mkdirs()

=======
  def getInput(filePathList: List[String]): Unit = {
    // 고정 크기의 스레드 풀 생성
>>>>>>> origin/shin
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessors))

    try {
      filePathList.zipWithIndex.foreach { case (oneFile, fileIndex) =>
        val source = Source.fromFile(oneFile)
        try {
          val lineChunks = getLineChunks(source.getLines(), CHUNK_SIZE)

          lineChunks.zipWithIndex.foreach { case (chunk, chunkIndex) =>
<<<<<<< HEAD
            val chunkFilePath = directory + "/file${fileIndex}_chunk$chunkIndex"
=======
            // 원래 파일이 있는 디렉토리 기반으로 출력 파일 경로 생성
            val originalDir = new File(oneFile).getParent
            val chunkFilePath = s"$originalDir/file${fileIndex}_$chunkIndex.txt"
>>>>>>> origin/shin

            val chunkFile = new File(chunkFilePath)
            if (!chunkFile.getParentFile.exists()) chunkFile.getParentFile.mkdirs()

            val future = Future {
              val writer = new PrintWriter(chunkFile)
              try {
                chunk.foreach(writer.println)
              } finally {
                writer.close()
              }
            }

            Await.result(future, Duration.Inf)
          }
        } finally {
          source.close()
        }
      }
    } finally {
      ec.shutdown()
    }
  }

  private def getLineChunks(linesIterator: Iterator[String], chunkSize: Int): Iterator[List[String]] = {
    new Iterator[List[String]] {
      def hasNext: Boolean = linesIterator.hasNext
      def next(): List[String] = linesIterator.take(chunkSize).toList
    }
  }
}
