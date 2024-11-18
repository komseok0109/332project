package utils

import scopt.OParser
import com.typesafe.scalalogging.LazyLogging
import scala.annotation.tailrec

case class MasterConfig(numWorkers: Int = 1)

case class WorkerConfig(masterIP: String = "",
                        inputDirectories: Seq[String] = Seq.empty,
                        outputDirectory: String = "")

object ArgumentParser extends LazyLogging {
  def parseMasterArgs(args: Array[String]): Option[MasterConfig] = {
    val builder = OParser.builder[MasterConfig]
    val parser = {
      import builder._
      OParser.sequence
        programName("Master"),
        head("Master", "1.0"),
        arg[Int]("<# of workers>")
          .action((x, c) => c.copy(numWorkers = x))
          .text("Number of workers")
      )
    }
    OParser.parse(parser, args, MasterConfig())
  }

  def parseWorkerArgs(args: Array[String]): Option[WorkerConfig] = {
    @tailrec
    def parseArguments(args: List[String], masterIP: String, inputDirs: Seq[String], outputDir: String,
                  invalidArguments: Seq[String]): (String, Seq[String], String, Seq[String]) = {
      args match {
        case Nil => (masterIP, inputDirs, outputDir, invalidArguments)
        case "-I" :: rest =>
          val (newInputDirs, remainingArgs) = rest.span(!_.startsWith("-"))
          parseArguments(remainingArgs, masterIP, inputDirs ++ newInputDirs, outputDir, invalidArguments)
        case "-O" :: value :: rest =>
          parseArguments(rest, masterIP, inputDirs, value, invalidArguments)
        case value :: rest if masterIP.isEmpty =>
          parseArguments(rest, value, inputDirs, outputDir, invalidArguments)
        case value :: rest =>
          parseArguments(rest, masterIP, inputDirs, outputDir, invalidArguments :+ s"Invalid argument: $value")
      }
    }

    val (masterIP, inputDirs, outputDir, invalidArguments) = parseArguments(args.toList, "", Seq(), "", Seq())

    val finalInvalidArguments = invalidArguments ++
      (if (masterIP.isEmpty) Seq("Master IP is required") else Seq()) ++
      (if (inputDirs.isEmpty) Seq("-I (input directories) are required") else Seq()) ++
      (if (outputDir.isEmpty) Seq("-O (output directory) is required") else Seq())

    if (finalInvalidArguments.nonEmpty) {
      finalInvalidArguments.foreach(msg => logger.error(msg))
      None
    } else {
      Some(WorkerConfig(masterIP, inputDirs, outputDir))
    }
  }

}
