package utils

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.LazyLogging
import java.io.IOException

object IOUtils extends LazyLogging {
  def getFilePathsFromDirectories(directories: List[String]): List[String] = {
    directories.flatMap { dir =>
      val path = Paths.get(dir)
      if (Files.exists(path) && Files.isDirectory(path)) {
        Files.walk(path)
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .map(_.toString)
          .toList
      } else {
        logger.error(s"$dir is not a valid directory or does not exist.")
        throw new IllegalArgumentException(s"Invalid directory: $dir")
      }
    }
  }

  def deleteFiles(filePaths: List[String]): Unit = {
    filePaths.foreach { filePath =>
      val path = Paths.get(filePath)
      if (Files.exists(path)) {
        try {
          Files.delete(path)
        } catch {
          case e: IOException => logger.error(s"Failed to delete $filePath: ${e.getMessage}")
        }
      } else {
        logger.warn(s"File does not exist: $filePath")
      }
    }
  }
}