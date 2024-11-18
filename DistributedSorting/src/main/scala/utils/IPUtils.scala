package utils

import java.net.InetAddress
import com.typesafe.scalalogging.LazyLogging

object IPUtils extends LazyLogging {
  def getMachineIP: String = {
    try {
      InetAddress.getLocalHost.getHostAddress
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get machine IP: ${e.getMessage}")
        "unknown"
    }
  }
}