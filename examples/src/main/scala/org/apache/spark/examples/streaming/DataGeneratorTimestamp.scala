import java.io.IOException
import java.net.ServerSocket

import org.apache.spark.util.IntParam
import org.apache.spark.streaming.util.RateLimitedOutputStream

/**
 * A helper program that sends timestamps one at a time in plain text at a specified rate.
 *
 * Usage: DataGeneratorTimestamp <port> <bytesPerSec>
 *   <port> is the port on localhost to run the generator
 *   <bytesPerSec> is the number of bytes the generator will send per second
 */
object DataGeneratorTimestamp {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: DataGeneratorTimestamp <port> <bytesPerSec>")
      System.exit(1)
    }
    // Parse the arguments using a pattern match
    val (port, bytesPerSec) = (args(0).toInt, args(1).toInt)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")
      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
      var counter = 0
      try {
        while (true) {
          val curTimeString = System.currentTimeMillis.toString
          val sb = new StringBuilder

          //for (i <- 0 until 1000/(curTimeString.length)) {
          for (i <- 0 until 10) {
              sb ++= curTimeString + "-" + counter.toString + "-" + port.toString + "\n"
              counter += 1
          }
          //sb ++= "\n"

          out.write(sb.toString.getBytes)
        }
      } catch {
        case e: IOException =>
          println("Client disconnected")
          socket.close()
      }
    }
  }
}

