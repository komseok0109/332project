import io.grpc._
import com.typesafe.scalalogging.LazyLogging
import message._
import utils._
import scala.concurrent._

object Worker extends LazyLogging {
  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
    ArgumentParser.parseWorkerArgs(args) match {
      case Some(config) =>
        logger.info("Starting worker with configuration: " + config)
        val worker = new Worker(config.masterIP.split(":")(0), config.masterIP.split(":")(1).toInt,
         config.inputDirectories, config.outputDirectory)(ec)
        worker.run()
      case None =>
        logger.error("Invalid arguments.")
    }
  }
}

class Worker(masterHost: String, masterPort: Int,
             inputDirectories: Seq[String], outputDirectory: String)(implicit ec: ExecutionContext)
  extends LazyLogging {
  private val channel = ManagedChannelBuilder
    .forAddress(masterHost, masterPort).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
  private val stub = MessageGrpc.blockingStub(channel)
  private var workerID: Option[Int] = None
  private var totalWorkerCount: Option[Int] = None

  def run(): Unit = {
    logger.info("Connect to Server: " + masterHost + ":" + masterPort)
    try {
      registerWithMaster()
    } catch {
      case e: Exception =>
        logger.error(s"Registration Error: ${e.getMessage}")
        shutDownChannel()
        System.exit(1)
    }
    shutDownChannel()
  }

  private def shutDownChannel(): Unit = {
    channel.shutdownNow()
  }

  private def registerWithMaster(): Unit = {
    IPUtils.getMachineIP match {
      case "unknown" => throw new RuntimeException("Failed to get IP")
      case ip =>
        val request = RegisterWorkerRequest(workerIP = ip)
        try {
          val reply = stub.registerWorker(request)
          workerID = Some(reply.workerID)
          totalWorkerCount = Some(reply.totalWorkerCount)
          logger.info(s"Successfully registered with worker ID: ${reply.workerID} and total workers: ${reply.totalWorkerCount}")
        } catch {
          case e: Exception =>
            logger.error(s"Failed to register worker: ${e.getMessage}")
            throw new RuntimeException("Worker registration failed, terminating program")
        }
    }
  }
}

