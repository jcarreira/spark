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

import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object TwitterGrepSocket {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: TwitterGrepSocket <consumer key> <consumer secret> " +
        "<access token> <access token secret> <MillisecondsWindow> <node> [<filters>]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterGrepSocket")
    val ssc = new StreamingContext(sparkConf, Milliseconds(args(4).toInt))

    val node = args(5)
    val stream = ssc.socketTextStream(node, 55555,
                            StorageLevel.MEMORY_ONLY_SER)

    //val englishTweets = stream.map(_.getText())
    //val englishTweets = stream.map(_.getInReplyToScreenName())
    val finalTweets = stream.filter(_.contains("1337"))
    //val finalTweets = stream.filter( record => {
    //   !( 
    //     (record == "null") || 
    //     (record == null) ||
    //     (record contains "null")
    //    )
    //})

    finalTweets.count()
    finalTweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
