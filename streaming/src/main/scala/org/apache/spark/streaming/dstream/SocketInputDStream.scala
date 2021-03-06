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

package org.apache.spark.streaming.dstream

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator
import org.apache.spark.SparkConf

import scala.reflect.ClassTag

import java.io._
import java.net.{UnknownHostException, Socket}
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

private[streaming]
class SocketInputDStream[T: ClassTag](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[T](ssc_) {

  def getReceiver(): Receiver[T] = {
    new SocketReceiver(host, port, bytesToObjects, storageLevel)
  }
}

private[streaming]
class SocketReceiver[T: ClassTag] (
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends Receiver[T](storageLevel) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  def receive() {
    var socket: Socket = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)
      val iterator = bytesToObjects(socket.getInputStream())
      while(!isStopped && iterator.hasNext) {
        val now = System.currentTimeMillis
        val value = iterator.next


        var first = ""
        var last = ""
        value match {
            case value:String => {
                first = value.substring(0,13)
                last = value.substring(value.length-13, value.length)
            }
        }
        store(value)
        val afterStore = System.currentTimeMillis
        logInfo(s"SocketInputDStream received now: $now first: " +
                s"$first last: $last storeTime: ${(afterStore - now)}")
      }
      logInfo("Stopped receiving")
      restart("Retrying connecting to " + host + ":" + port)
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    } finally {
      if (socket != null) {
        socket.close()
        logInfo("Closed socket to " + host + ":" + port)
      }
    }
  }
}

private[streaming]
object SocketReceiver extends Logging {
  //val conf = new SparkConf;
  //val aggregationFactor = conf.getInt("spark.streaming.aggregationFactor", 0)

  /**
   * This methods translates the data from an inputstream (say, from a socket)
   * to '\n' delimited strings and returns an iterator to access the strings.
   */
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = 
        new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))//, 10000000)
    new NextIterator[String] {
      protected override def getNext() = {
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close() {
        dataInputStream.close()
      }
    }
  }

  object AllDone extends Exception { }
  def bytesToLines2(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = 
        new BufferedReader(new InputStreamReader(inputStream, "UTF-8"), 100000)
    new NextIterator[String] {
      protected override def getNext() = {
        var nextValue = dataInputStream.readLine()

        var time = System.currentTimeMillis
        try {
           var count = 1;
           (1 to 10000).foreach( 
                   it => {
                     val new_value = dataInputStream.readLine() 
                     if (new_value == null) {
                        throw AllDone
                     }
                     nextValue += new_value
                     if (count % 10 == 0 && System.currentTimeMillis - time > 5) {
                       throw AllDone
                     }
                     count += 1
                })
        } catch {
           case AllDone =>
        }


        logInfo(s"Received block. Took ${(System.currentTimeMillis - time)}")

        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close() {
        dataInputStream.close()
      }
    }
  }

}
