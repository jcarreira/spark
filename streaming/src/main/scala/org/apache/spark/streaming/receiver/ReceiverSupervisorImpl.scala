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

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await

import akka.actor.{Actor, Props}
import akka.pattern.ask
import com.google.common.base.Throwables
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Logging, SparkEnv, SparkException}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Concrete implementation of [[org.apache.spark.streaming.receiver.ReceiverSupervisor]]
 * which provides all the necessary functionality for handling the data received by
 * the receiver. Specifically, it creates a [[org.apache.spark.streaming.receiver.BlockGenerator]]
 * object that is used to divide the received data stream into blocks of data.
 */
private[streaming] class ReceiverSupervisorImpl(
    receiver: Receiver[_],
    env: SparkEnv,
    hadoopConf: Configuration,
    checkpointDirOption: Option[String]
  ) extends ReceiverSupervisor(receiver, env.conf) with Logging {

  private val receivedBlockHandler: ReceivedBlockHandler = {
    if (env.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false)) {
      if (checkpointDirOption.isEmpty) {
        throw new SparkException(
          "Cannot enable receiver write-ahead log without checkpoint directory set. " +
            "Please use streamingContext.checkpoint() to set the checkpoint directory. " +
            "See documentation for more details.")
      }
      new WriteAheadLogBasedBlockHandler(env.blockManager, receiver.streamId,
        receiver.storageLevel, env.conf, hadoopConf, checkpointDirOption.get)
    } else {
      new BlockManagerBasedBlockHandler(env.blockManager, receiver.storageLevel)
    }
  }


  /** Remote Akka actor for the ReceiverTracker */
  private val trackerActor = {
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = "akka.tcp://%s@%s:%s/user/ReceiverTracker".format(
      SparkEnv.driverActorSystemName, ip, port)
    env.actorSystem.actorSelection(url)
  }

  /** Timeout for Akka actor messages */
  private val askTimeout = AkkaUtils.askTimeout(env.conf)

  /** Akka actor for receiving messages from the ReceiverTracker in the driver */
  private val actor = env.actorSystem.actorOf(
    Props(new Actor {
      override def preStart() {
        logInfo("Registered receiver " + streamId)
        val msg = RegisterReceiver(
          streamId, receiver.getClass.getSimpleName, Utils.localHostName(), self)
        val future = trackerActor.ask(msg)(askTimeout)
        Await.result(future, askTimeout)
      }

      override def receive() = {
        case StopReceiver =>
          logInfo("Received stop signal")
          stop("Stopped by driver", None)
      }

      def ref = self
    }), "Receiver-" + streamId + "-" + System.currentTimeMillis())

  /** Unique block ids if one wants to add blocks directly */
  private val newBlockId = new AtomicLong(System.currentTimeMillis())

  /** Divides received data records into data blocks for pushing in BlockManager. */
  private val blockGenerator = new BlockGenerator(new BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      //val now = System.currentTimeMillis
      //logInfo(s"onPushBlock now: $now")

      //ArrayBuffer match {
      //  case arrayBuffer: ArrayBuffer[String] => {
      //    val str = arrayBuffer.head
      //    logInfo(s"onPushBlock is String: $str!!")
      //  }
      //  case _ => logInfo("onPushBlock is unknown!!")
      //}

      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }, streamId, env.conf)

  /** Push a single record of received data into block generator. */
  def pushSingle(data: Any) {
    blockGenerator.addData(data)
  }

  /** Store an ArrayBuffer of received data as a data block into Spark's memory. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)
  }

  /** Store a iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(IteratorBlock(iterator), metadataOption, blockIdOption)
  }

  /** Store the bytes of received data as a data block into Spark's memory. */
  def pushBytes(
      bytes: ByteBuffer,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ByteBufferBlock(bytes), metadataOption, blockIdOption)
  }

  def f[T](v: T)(implicit ev: ClassTag[T]) = ev.toString
  /** Store block and report it to driver */
  def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    val time = System.currentTimeMillis
    val blockId = blockIdOption.getOrElse(nextBlockId)
    var last_record = ""
    var head_record = ""

    val numRecords = receivedBlock match {
      case ArrayBufferBlock(arrayBuffer) => {
          arrayBuffer match {
              case array:ArrayBuffer[String] => {
                val last = array.last;
                val head = array.head;

                head_record = head.substring(0, 13)
                last_record = last.substring(last.length - 13, last.length)
                //last_record = last
              }
              arrayBuffer.size
         }
      }
      case _ => -1
    }

    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)

    //logInfo(s"ReceiverSupervisorImpl - block reported at $time")

    //val array = receivedBlock match {
    //    case ArrayBufferBlock(arrayBuffer) => arrayBuffer
    //}   
    ////val last = array.last
    //
    //logInfo("ReceiverSupervisorImpl last type: " + f(array.last))    

    //val valueBytes = (array.last match {
    //          case Some(x:Array[Byte]) => logInfo("ReceiverSupervisorImpl is Array[byte]")
    //          case Some(x:ByteBuffer) => logInfo("ReceiverSupervisorImpl is ByteBuffer")
    //          case Some(x:Array[ByteBuffer]) => 
    //                 logInfo("ReceiverSupervisorImpl is Array[ByteBuffer")
    //          case Some(x:String) => logInfo("ReceiverSupervisorImpl is String")
    //            case _ => logInfo("ReceiverSupervisorImpl is unknown")
    //            })
    //val valueStr = new String(valueBytes)
    //val value = -1
    //logInfo(s"ReceiverSupervisorImpl - Block generated at $value and reported at $time")

    val last_record_long = last_record.toLong
    logInfo(s"ReceivedBlockInfo last record: $last_record $last_record_long")
                
    logInfo(s"pushAndReportBlock: Storing arraybuffer $head_record " +
            s"$last_record ${(System.currentTimeMillis)}")

    val blockInfo = ReceivedBlockInfo(streamId, numRecords, 
                 blockStoreResult, last_record.toLong)
    val future = trackerActor.ask(AddBlock(blockInfo))(askTimeout)
        //Await.result(future, askTimeout)
    //    logDebug(s"Reported block $blockId")
    logInfo(s"Pushed block $blockId in ${(System.currentTimeMillis - time)} ms")
  }

  /** Report error to the receiver tracker */
  def reportError(message: String, error: Throwable) {
      val errorString = Option(error).map(Throwables.getStackTraceAsString).getOrElse("")
          trackerActor ! ReportError(streamId, message, errorString)
          logWarning("Reported error " + message + " - " + error)
  }

  override protected def onStart() {
      blockGenerator.start()
  }

  override protected def onStop(message: String, error: Option[Throwable]) {
      blockGenerator.stop()
          env.actorSystem.stop(actor)
  }

  override protected def onReceiverStart() {
      val msg = RegisterReceiver(
              streamId, receiver.getClass.getSimpleName, Utils.localHostName(), actor)
          val future = trackerActor.ask(msg)(askTimeout)
          Await.result(future, askTimeout)
  }

  override protected def onReceiverStop(message: String, error: Option[Throwable]) {
      logInfo("Deregistering receiver " + streamId)
          val errorString = error.map(Throwables.getStackTraceAsString).getOrElse("")
          val future = trackerActor.ask(
                  DeregisterReceiver(streamId, message, errorString))(askTimeout)
          Await.result(future, askTimeout)
          logInfo("Stopped receiver " + streamId)
  }

  /** Generate new block ID */
  private def nextBlockId = StreamBlockId(streamId, newBlockId.getAndIncrement)
}
