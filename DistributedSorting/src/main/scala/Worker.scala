import io.grpc.ManagedChannelBuilder
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging
import message._
import utils._

object Worker extends LazyLogging {
  def main(args: Array[String]): Unit = {
    ArgumentParser.parseWorkerArgs(args) match {
      case Some(config) =>
        logger.info("Starting worker with configuration: " + config)
        val worker = new Worker(config.masterIP.split(":")(0), config.masterIP.split(":")(1).toInt,
         config.inputDirectories, config.outputDirectory)
        worker.run()
      case None =>
        logger.error("Invalid arguments.")
    }
  }

}

class Worker(masterHost: String, masterPort: Int,
             inputDirectories: Seq[String], outputDirectory: String) extends LazyLogging {
  private val channel = ManagedChannelBuilder
    .forAddress(masterHost, masterPort).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
  private val stub = MessageGrpc.stub(channel)

  def run(): Unit = {
    logger.info("Connect to Server: " + masterHost + ":" + masterPort)
    shutDownChannel()
  }

  private def shutDownChannel(): Unit = {
    channel.shutdownNow()
  }
}

