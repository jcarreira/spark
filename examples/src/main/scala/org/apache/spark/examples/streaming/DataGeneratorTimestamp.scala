/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming

import java.io.IOException
import java.net.ServerSocket
import org.apache.spark.streaming.util.RateLimitedOutputStream

import org.apache.spark.util.IntParam

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
      try {

        var timeBegin = System.currentTimeMillis
        var sentBytes = 0

        while (true) {

          if (out.haveToWait()) {
            val toWait = out.timeToWait();
            println("Sleeping ms: " + toWait)
            Thread.sleep(toWait + 5)
          }

          val curTime = System.currentTimeMillis
          var curTimeString = ""
          (1 to 1000).foreach(id => curTimeString += curTime.toString + "\n")

          sentBytes += curTimeString.length
          out.write(curTimeString.getBytes)

          // a second passed?
          if (System.currentTimeMillis - timeBegin >= 1000) {
              println("Last sec sent bytes: " + sentBytes)
                sentBytes = 0
                timeBegin = System.currentTimeMillis
          }
        }
      } catch {
        case e: IOException =>
          println("Client disconnected")
          socket.close()
      }
    }
  }
}

