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

/* XXX Figure out how to build this */
import RemoteBuf

/**
 * Remote Memory (disaggregation) Store
 *
 * Behaves as a DiskStore but tries to use remote memory instead of a disk. For now this assumes that RMEM is
 * inexhaustible, eventually we'll do something more clever.
 */
private[spark] class RmemStore(conf: SparkConf, diskManager: DiskBlockManager) extends Logging {

  RemoteBuf.BufferManager BM = RemoteBuf.BufferManager()

  def getSize(blockId: BlockId): Long = {
    logTrace("\"RMEM\" getSize()")

    try {
      BM.GetBuffer(blockId.name).GetSize()
    } catch {
      /* The only exception possible is that the buffer doesn't exist, return 0 to mimic DiskStore */
      case _ => 0L
    }
  }

  def put(blockId: BlockId)(writeFunc: Java.io.OutputStream => Unit): Unit = {
    logTrace("\"RMEM\" put()")

    if (BM.BufferExists(blockId.name)) {
      throw new IllegalStateException(s"Block $blockId is already present in the RMRM store")
    }

    logDebug(s"Attempting to put block $blockId in RMEM")
    val startTime = System.currentTimeMillis

    val RBuf = BM.CreateBuffer(blockId.name)
    val RBufStream = RBuf.GetStream()

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
    /* XXX I'm not clear on if closing RBufStream already does this... */
    RBuf.WriteDone()

    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored to RMEM in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    logTrace("\"RMEM\" putBytes()")

    /* This is nasty copy-pasta from put(). I should really come up with a way to do this better... */
    if (BM.BufferExists(blockId.name)) {
      throw new IllegalStateException(s"Block $blockId is already present in the RMRM store")
    }

    logDebug(s"Attempting to put block $blockId in RMEM")
    val startTime = System.currentTimeMillis

    val RBuf = BM.CreateBuffer(blockId.name)
    val RBufChan = RBuf.GetChannel()

    var threwException: Boolean = true
    try {
      bytes.writeFully(RBufChan)
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
    /* XXX I'm not clear on if closing RBufStream already does this... */
    RBuf.WriteDone()

    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored to RMEM in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    logTrace("\"RMEM\" getBytes()")
    try {
      val RBuf = BM.GetBuffer(blockId.name)
    } catch {
      /* this mimics the behavior of DiskStore by creating an empty buffer */
      val RBuf = BM.CreateBuffer(blockId.name)
    }

    val localBuf = ByteBuffer.allocate(RBuf.GetSize())
    try {
      RBuf.Read(localBuf)
    } catch {
      case _ => throw new IOException("Failed while reading block " + blockId.name + " from RMEM")
    }

    new ChunkedByteBuffer(localBuf)

  }

  def remove(blockId: BlockId): Boolean = {
    logTrace("\"RMEM\" remove()")
    BM.DestroyBuffer(blockId.name)
  }

  def contains(blockId: BlockId): Boolean = {
    logTrace("\"RMEM\" contains()")
    BM.BufferExists(blockId.name)
  }

}
