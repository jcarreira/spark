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

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.util.IntParam
import org.apache.spark.streaming.util.RateLimitedOutputStream

/**
 * Receives text from multiple socketTextStreams of timestamps, compute
 * timestamp differences, and print the results.
 * Note that no serialization is used in this benchmark.
 *
 * Usage: BenchmarkTimestamp <numStreams> <host> <port> <batchMillis> [blockInterval]
 *   <numStream> is the number socketTextStreams, which should be same as number
 *               of work nodes in the cluster
 *   <host> is the source of the input stream (usually "localhost")
 *   <port> is the port on which the input stream is running on host
 *   <batchMillise> is the Spark Streaming batch duration in milliseconds
 *   [blockInterval] is the Spark Streaming block interval in milliseconds (default is 200)
 */
object BenchmarkTimestamp {
  def main(args: Array[String]) {
    if (args.length != 4 && args.length != 5) {
      System.err.println("Usage: BenchmarkTimestamp <numStreams> <host> <port> <batchMillis> [blockInterval]")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis) = (args(0).toInt, args(1), args(2).toInt, args(3).toInt)
    val blockInterval = if (args.length == 5) args(4) else None
    val sparkConf = new SparkConf()
    sparkConf.setAppName("BenchMarkTimestamp")
    sparkConf.set("spark.eventLog.enabled","true")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    if (blockInterval != None) sparkConf.set("spark.streaming.blockInterval", blockInterval.toString)
    if (sparkConf.getOption("spark.master") == None) {
      // Master not set, as this was not launched through Spark-submit. Setting master as local."
      sparkConf.setMaster("local[*]")
    }

    // Create the context
    val ssc = new StreamingContext(sparkConf, Duration(batchMillis))

    val times = ssc.socketTextStream(host, port, StorageLevel.MEMORY_ONLY_SER)
    val latencies = times.map{times =>
      var result = "" 
      times.split("\n").foreach( time => {
         val receiveTime = System.currentTimeMillis;
         val sendTime = time.substring(0,13).toLong;
         val latency = receiveTime - sendTime;
         result += s"time: $time $receiveTime $latency\n"
      })
      result
    }
    //latencies.saveAsTextFiles("latencies")
    latencies.print()
    //latencies.count.map(c => s"$c records").print()

    ssc.start()
    ssc.awaitTermination()
  }
}

