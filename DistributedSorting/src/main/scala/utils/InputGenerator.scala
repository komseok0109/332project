package utils

import utils.IOUtils.{deleteFiles, getFilePathsFromDirectories}

import scala.io.Source
import java.io.{File, PrintWriter}
import scala.concurrent._
import java.util.concurrent.Executors
import scala.concurrent.duration._

object InputGenerator {
  def main(args: Array[String]): Unit = {
    val filePaths = getFilePathsFromDirectories(args.toList)
    generateInput(filePaths)
    deleteFiles(filePaths)
  }
  private val CHUNK_SIZE = 320000
  private def generateInput(filePathList: List[String]): Unit = {
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    implicit val ec: ExecutionContextExecutorService
      = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessors))

    try {
      filePathList.zipWithIndex.foreach { case (oneFile, fileIndex) =>
        val source = Source.fromFile(oneFile)
        try {
          val lineChunks = getLineChunks(source.getLines(), CHUNK_SIZE)

          lineChunks.zipWithIndex.foreach { case (chunk, chunkIndex) =>
            val originalDir = new File(oneFile).getParent
            val chunkFilePath = s"$originalDir/file${fileIndex}_$chunkIndex.txt"

            val chunkFile = new File(chunkFilePath)
            if (!chunkFile.getParentFile.exists()) chunkFile.getParentFile.mkdirs()

            val future = Future {
              val writer = new PrintWriter(chunkFile)
              try {
                chunk.foreach(writer.println)
              } finally {
                writer.close()
              }
              println(s"File $fileIndex, Chunk $chunkIndex 저장 완료: $chunkFilePath")
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