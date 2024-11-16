import io.grpc.ManagedChannelBuilder
import scala.concurrent.duration._
import message._

object Worker {
  def main(args: Array[String]): Unit = {
    val channel = ManagedChannelBuilder.forAddress("localhost", 50051)
      .usePlaintext()
      .build()

    val stub = MessageGrpc.stub(channel)

    try {
      println("Client is running...")
    } finally {
      channel.shutdownNow()
      if (!channel.awaitTermination(5, SECONDS)) {
        println("Channel did not shut down properly")
      }
    }
  }
}