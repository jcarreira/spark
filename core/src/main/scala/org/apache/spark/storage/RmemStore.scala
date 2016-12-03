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

package org.apache.spark.storage

import java.io.IOException
import java.nio.ByteBuffer

import com.google.common.io.Closeables
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.io.ChunkedByteBuffer

import ucb.remotebuf._

/**
 * Remote Memory (disaggregation) Store
 *
 * Behaves as a DiskStore but tries to use remote memory instead of a disk. For now this assumes that RMEM is
 * inexhaustible, eventually we'll do something more clever.
 */
private[spark] class RmemStore(conf: SparkConf, diskManager: DiskBlockManager) extends Logging {

  val BM = new RemoteBuf.BufferManager()

  def getSize(blockId: BlockId): Long = {
    logTrace("\"RMEM\" getSize()")

    try {
      BM.getBuffer(blockId.name).getSize()
    } catch {
      /* The only exception possible is that the buffer doesn't exist, return 0 to mimic DiskStore */
     case _: Throwable => 0L
    }
  }

  def put(blockId: BlockId)(writeFunc: java.io.OutputStream => Unit): Unit = {
    logTrace("\"RMEM\" put()")

    if (BM.bufferExists(blockId.name)) {
      throw new IllegalStateException(s"Block $blockId is already present in the RMRM store")
    }

    logDebug(s"Attempting to put block $blockId in RMEM")
    val startTime = System.currentTimeMillis

    val RBuf = BM.createBuffer(blockId.name)
    val RBufStream = new ROutputStream(RBuf)

    var threwException: Boolean = true
    try {
      writeFunc(RBufStream)
      threwException = false
    } finally {
      try {
        Closeables.close(RBufStream, threwException)
      } finally {
        if (threwException) {
          remove(blockId)
        }
      }
    }

    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored to RMEM in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    logTrace("\"RMEM\" putBytes()")

    /* This is nasty copy-pasta from put(). I should really come up with a way to do this better... */
    if (BM.bufferExists(blockId.name)) {
      throw new IllegalStateException(s"Block $blockId is already present in the RMRM store")
    }

    logDebug(s"Attempting to put block $blockId in RMEM")
    val startTime = System.currentTimeMillis

    val RBuf = BM.createBuffer(blockId.name)
    val RBufChan = new RWritableByteChannel(RBuf)

    var threwException: Boolean = true
    try {
      bytes.writeFully(RBufChan)
      threwException = false
    } finally {
      try {
        Closeables.close(RBufChan, threwException)
      } finally {
        if (threwException) {
          remove(blockId)
        }
      }
    }

    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored to RMEM in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    logTrace("\"RMEM\" getBytes()")
    val RBuf = try {
      BM.getBuffer(blockId.name)
    } catch {
      /* this mimics the behavior of DiskStore by creating an empty buffer */
     case _: Throwable => BM.createBuffer(blockId.name)
    }

    val localBuf = ByteBuffer.allocate(RBuf.getSize())
    try {
      RBuf.read(localBuf)
    } catch {
      case _: Throwable => throw new IOException("Failed while reading block " + blockId.name + " from RMEM")
    }

    new ChunkedByteBuffer(localBuf)
  }

  def remove(blockId: BlockId): Boolean = {
    logTrace("\"RMEM\" remove()")
    if(this.contains(blockId)) {
      try {
        BM.deleteBuffer(blockId.name)
        true
      } catch {
        case _: Throwable => false
      }
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    logTrace("\"RMEM\" contains()")
    BM.bufferExists(blockId.name)
  }

}
