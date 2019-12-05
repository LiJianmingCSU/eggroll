/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.format

import java.io._
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.io.adapter.{BlockDeviceAdapter, FileBlockAdapter, HdfsBlockAdapter}
import com.webank.eggroll.core.io.util.IoUtils
import com.webank.eggroll.core.meta.{ErPartition, ErStore}
import com.webank.eggroll.rollframe.NioTransferEndpoint
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter, ReadChannel}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer


object FrameUtils {
  /**
    * convert bytes data to FrameBatch
    *
    * @param bytes bytes data
    * @return
    */
  def fromBytes(bytes: Array[Byte]): FrameBatch = {
    val input = new ByteArrayInputStream(bytes)
    val cr = new FrameReader(input)
    cr.getColumnarBatches().next()
  }

  /**
    * convert FrameBatch to bytes data
    *
    * @param cb FrameBatch
    * @return
    */
  def toBytes(cb: FrameBatch): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val cw = new FrameWriter(cb, output)
    cw.write()
    val ret = output.toByteArray
    cw.close()
    ret
  }

  /**
    * copy a new FrameBatch with serialization
    *
    * @param fb FrameBatch
    * @return
    */
  @deprecated("Use `FrameUtils.fork() instead`")
  def copy(fb: FrameBatch): FrameBatch = {
    fromBytes(toBytes(fb))
  }

  /**
    * copy a new FrameBatch with arrow transferPair
    * @param fb FrameBatch
    * @return new FrameBatch
    */
  def fork(fb: FrameBatch): FrameBatch = {
    val transfer: VectorSchemaRoot = {
      val sliceVectors = fb.rootSchema.arrowSchema.getFieldVectors.asScala.map((v: FieldVector) => {
        def foo(v: FieldVector) = {
          val transferPair = v.getTransferPair(v.getAllocator)
          transferPair.transfer()
          transferPair.getTo.asInstanceOf[FieldVector]
        }
        foo(v)
      }).asJava
      new VectorSchemaRoot(sliceVectors)
    }
    new FrameBatch(new FrameSchema(transfer))
  }

}

// TODO: where to delete a RollFrame?
trait FrameDB {
  def close(): Unit

  def readAll(): Iterator[FrameBatch]

  def writeAll(batches: Iterator[FrameBatch]): Unit

  def append(batch: FrameBatch): Unit = writeAll(Iterator(batch))

  def readOne(): FrameBatch = readAll().next() // need to check iterator whether hasNext element
}

object FrameDB {
  val FILE: String = StringConstants.FILE
  val CACHE: String = StringConstants.CACHE
  val HDFS: String = StringConstants.HDFS
  val NETWORK: String = StringConstants.NETWORK
  val QUEUE: String = StringConstants.QUEUE
  val TOTAL: String = StringConstants.TOTAL

  val PATH: String = StringConstants.PATH
  val TYPE: String = StringConstants.TYPE
  val HOST: String = StringConstants.HOST
  val PORT: String = StringConstants.PORT

  private val rootPath = "/tmp/unittests/RollFrameTests/"

  private def getStorePath(store: ErStore, partitionId: Int): String = {
    val storeLocator = store.storeLocator
    val path = storeLocator.path
    val dir =
      if (StringUtils.isBlank(path))
        s"$rootPath/${storeLocator.toPath()}"
      else
        path

    s"$dir/$partitionId"
  }

  private def getStorePath(partition: ErPartition): String = {
    IoUtils.getPath(partition, rootPath)
  }

  def apply(opts: Map[String, String]): FrameDB = opts.getOrElse(TYPE, FILE) match {
    case CACHE => new JvmFrameDB(opts(PATH))
    case QUEUE => new QueueFrameDB(opts(PATH), opts(TOTAL).toInt)
    case HDFS => new HdfsFrameDB(opts(PATH))
    case NETWORK => new NetworkFrameDB(opts(PATH), opts(HOST), opts(PORT).toInt)
    case _ => new FileFrameDB(opts(PATH))
  }

  /**
    * if want to support network FrameDB, it must has process address,so ErStore must contain full partition message.
    *
    * @param store       : FrameBatch store
    * @param partitionId : 0,1,2 ...
    * @return
    */
  def apply(store: ErStore, partitionId: Int): FrameDB =
    apply(Map(PATH -> getStorePath(store, partitionId), TYPE -> store.storeLocator.storeType,
      HOST -> store.partitions(partitionId).processor.dataEndpoint.host,
      PORT -> store.partitions(partitionId).processor.dataEndpoint.port.toString))

  def apply(partition: ErPartition): FrameDB = {
    apply(Map(PATH -> getStorePath(partition), TYPE -> partition.storeLocator.storeType,
      HOST -> partition.processor.dataEndpoint.host, PORT -> partition.processor.dataEndpoint.port.toString))
  }

  def queue(path: String, total: Int): FrameDB = apply(Map(PATH -> path, TOTAL -> total.toString, TYPE -> QUEUE))

  def file(path: String): FrameDB = apply(Map(PATH -> path, TYPE -> FILE))

  def cache(path: String): FrameDB = apply(Map(PATH -> path, TYPE -> CACHE))

  def hdfs(path: String): FrameDB = apply(Map(PATH -> path, TYPE -> HDFS))

  def network(path: String, host: String, port: String): FrameDB = apply(Map(PATH -> path, TYPE -> NETWORK,
    HOST -> host, PORT -> port))
}

// NOT thread safe
class FileFrameDB(path: String) extends FrameDB {
  var frameReader: FrameReader = _
  var frameWriter: FrameWriter = _

  override def close(): Unit = {
    if (frameReader != null) frameReader.close()
    if (frameWriter != null) frameWriter.close()
  }

  override def readAll(): Iterator[FrameBatch] = {
    frameReader = new FrameReader(new FileBlockAdapter(path))
    frameReader.getColumnarBatches()
  }

  def readAll(nullableFields: Set[Int] = Set[Int]()): Iterator[FrameBatch] = {
    val dir = new File(path).getParentFile
    if (!dir.exists()) dir.mkdirs()
    frameReader = new FrameReader(new FileBlockAdapter(path))
    frameReader.nullableFields = nullableFields
    frameReader.getColumnarBatches()
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach { batch =>
      if (frameWriter == null) {
        frameWriter = new FrameWriter(batch, new FileBlockAdapter(path))
        frameWriter.write()
      } else {
        frameWriter.writeSibling(batch)
      }
    }
  }
}

class HdfsFrameDB(path: String) extends FrameDB {
  var frameReader: FrameReader = _
  var frameWriter: FrameWriter = _

  def close(): Unit = {
    if (frameReader != null) frameReader.close()
    if (frameWriter != null) frameWriter.close()
  }

  def readAll(): Iterator[FrameBatch] = {
    frameReader = new FrameReader(new HdfsBlockAdapter(path))
    frameReader.getColumnarBatches()
  }

  def readAll(nullableFields: Set[Int] = Set[Int]()): Iterator[FrameBatch] = {
    frameReader = new FrameReader(new HdfsBlockAdapter(path))
    frameReader.nullableFields = nullableFields
    frameReader.getColumnarBatches()
  }

  def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach { batch =>
      if (frameWriter == null) {
        frameWriter = new FrameWriter(batch, new HdfsBlockAdapter(path))
        frameWriter.write()
      } else {
        frameWriter.writeSibling(batch)
      }
    }
  }
}

object QueueFrameDB {
  private val map = TrieMap[String, BlockingQueue[FrameBatch]]()

  // key: a task name,e.g. mapBatch-0-doing , BlockingQueue[]: several batch FrameBatch
  def getOrCreateQueue(key: String): BlockingQueue[FrameBatch] = this.synchronized {
    if (!map.contains(key)) {
      map.put(key, new LinkedBlockingQueue[FrameBatch]())
    }
    map(key)
  }
}

class QueueFrameDB(path: String, total: Int) extends FrameDB {
  // TODO: QueueFrameStoreAdapter.getOrCreateQueue(path)
  override def close(): Unit = {}

  override def readAll(): Iterator[FrameBatch] = {
    require(total >= 0, "blocking queue need a total size before read")
    new Iterator[FrameBatch] {
      private var remaining = total

      override def hasNext: Boolean = remaining > 0

      override def next(): FrameBatch = {
        remaining -= 1
        println("taking from queue:" + path)
        val ret = QueueFrameDB.getOrCreateQueue(path).take()
        println("token from queue:" + path)
        ret
      }
    }
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach(QueueFrameDB.getOrCreateQueue(path).put)
  }
}

class NetworkFrameDB(path: String, host: String, port: Int) extends FrameDB {
  // TODO: client need multi-thread ?
  var client: NioTransferEndpoint = _

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    // write FrameBatch to remote server
    if (client == null) {
      client = new NioTransferEndpoint()
      client.runClient(host, port)
    }
    batches.foreach(batch => client.send(path, batch))
  }

  override def readAll(): Iterator[FrameBatch] = {
    // read FrameBatch from local queue, if FrameBatch was used many time, must be loaded to cache
    new Iterator[FrameBatch] {
      override def hasNext: Boolean = {
        !QueueFrameDB.getOrCreateQueue(path).isEmpty
      }

      override def next(): FrameBatch = {
        println("taking from queue:" + path)
        val ret = QueueFrameDB.getOrCreateQueue(path).take
        println("token from queue:" + path)
        ret
      }
    }
  }

  override def close(): Unit = {
    // need to close ?
    //    client.clientChannel.close()
  }
}

object JvmFrameDB {
  private val caches: TrieMap[String, ListBuffer[FrameBatch]] = new TrieMap[String, ListBuffer[FrameBatch]]()
}

class JvmFrameDB(path: String) extends FrameDB {
  override def readAll(): Iterator[FrameBatch] = JvmFrameDB.caches(path).toIterator

  override def writeAll(batches: Iterator[FrameBatch]): Unit = this.synchronized {
    if (!JvmFrameDB.caches.contains(path)) {
      JvmFrameDB.caches.put(path, ListBuffer())
    }
    JvmFrameDB.caches(path).appendAll(batches)
  }

  // TODO : clear ?
  override def close(): Unit = {}
}

class ArrowStreamSiblingWriter(root: VectorSchemaRoot,
                               provider: DictionaryProvider,
                               out: WritableByteChannel,
                               var nullableFields: Set[Int] = Set[Int]())
  extends ArrowStreamWriter(root, provider, out: WritableByteChannel) {

  def this(root: VectorSchemaRoot, provider: DictionaryProvider, outStream: OutputStream) {
    this(root, provider, Channels.newChannel(outStream))
  }

  def writeSibling(sibling: VectorSchemaRoot): Unit = {
    val vu = new VectorUnloader(root)
    val batch = vu.getRecordBatch
    writeRecordBatch(batch)
  }
}

class ArrowStreamReusableReader(messageReader: MessageChannelReader,
                                allocator: BufferAllocator,
                                var nullableFields: Set[Int] = Set[Int]())
  extends ArrowStreamReader(messageReader, allocator) {

  def this(in: InputStream, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(Channels.newChannel(in)), allocator), allocator)
  }

  def this(in: ReadableByteChannel, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(in), allocator), allocator)
  }

  override def loadNextBatch(): Boolean =
    throw new UnsupportedOperationException("use loadNewBatch instead")

  // do not reset row count
  override def prepareLoadNextBatch(): Unit = {
    ensureInitialized()
  }

  // For concurrent usage
  def loadNewBatch(): VectorSchemaRoot = {
    prepareLoadNextBatch()
    val result = messageReader.readNext

    // Reached EOS
    if (result == null) return null

    if (result.getMessage.headerType != MessageHeader.RecordBatch)
      throw new IOException("Expected RecordBatch but header was " + result.getMessage.headerType)

    var bodyBuffer = result.getBodyBuffer

    // For zero-length batches, need an empty buffer to deserialize the batch
    if (bodyBuffer == null) bodyBuffer = allocator.getEmpty

    val batch = MessageSerializer.deserializeRecordBatch(result.getMessage, bodyBuffer)

    val dr = new ArrowDiscreteReader(this.getVectorSchemaRoot.getSchema, batch, allocator)
    dr.loadNextBatch()
    dr.getVectorSchemaRoot
  }
}

class ArrowDiscreteReader(schema: Schema, batch: ArrowRecordBatch, allocator: BufferAllocator)
  extends ArrowReader(allocator) {
  private var hasNext = true

  // once only
  override def loadNextBatch(): Boolean = {
    ensureInitialized()
    loadRecordBatch(batch)
    hasNext = false
    hasNext
  }

  override def bytesRead(): Long =
    throw new UnsupportedOperationException("use loadNextBatch")

  override def closeReadSource(): Unit =
    throw new UnsupportedOperationException("use loadNextBatch")

  override def readSchema(): Schema = schema

  //  override def readDictionary(): ArrowDictionaryBatch =
  //    throw new UnsupportedOperationException("use loadNextBatch")
}

class FrameWriter(val rootSchema: VectorSchemaRoot, val arrowWriter: ArrowStreamSiblingWriter) {

  var outputStream: OutputStream = _

  def this(rootSchema: VectorSchemaRoot, outputStream: OutputStream) {
    this(rootSchema,
      new ArrowStreamSiblingWriter(
        rootSchema,
        new DictionaryProvider.MapDictionaryProvider(),
        outputStream
      )
    )
    this.outputStream = outputStream
  }

  def this(rootSchema: VectorSchemaRoot, channel: WritableByteChannel) {
    this(rootSchema,
      new ArrowStreamSiblingWriter(
        rootSchema,
        new DictionaryProvider.MapDictionaryProvider(),
        channel
      )
    )
  }

  def this(frameBatch: FrameBatch, adapter: BlockDeviceAdapter) {
    this(frameBatch.rootSchema.arrowSchema, adapter.getOutputStream())
  }

  def this(frameBatch: FrameBatch, outputStream: OutputStream) {
    this(frameBatch.rootSchema.arrowSchema, outputStream)
  }

  def this(frameBatch: FrameBatch, channel: WritableByteChannel) {
    this(frameBatch.rootSchema.arrowSchema, channel)
  }

  def this(rootSchema: FrameSchema, adapter: BlockDeviceAdapter) {
    this(rootSchema.arrowSchema, adapter.getOutputStream())
  }

  def close(closeStream: Boolean = true): Unit = {
    // Output Stream can be closed in arrowWriter ?
    arrowWriter.end()
    if (closeStream) {
      arrowWriter.close()
      if (outputStream != null) outputStream.close()
    }
  }

  def write(valueCount: Int, batchSize: Int, f: (Int, FrameVector) => Unit): Unit = {
    arrowWriter.start()
    for (b <- 0 until (valueCount + batchSize - 1) / batchSize) {
      val rowCount = if (valueCount < batchSize) valueCount else batchSize
      rootSchema.setRowCount(rowCount)
      for (fieldIndex <- 0 until rootSchema.getSchema.getFields.size()) {
        val vector = rootSchema.getFieldVectors.get(fieldIndex)
        try {
          vector.setInitialCapacity(rowCount)
          vector.allocateNew()
          f(fieldIndex, new FrameVector(vector))
          vector.setValueCount(rowCount)
        } finally {
          // todo: should be closed with config?
          // vect.close()
        }
      }
      arrowWriter.writeBatch()
      println("batch index", b)
    }
  }

  // existed vectors
  def write(): Unit = {
    rootSchema.setRowCount(rootSchema.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeBatch()
  }

  def writeSibling(columnarBatch: FrameBatch): Unit = {
    val sibling = new VectorSchemaRoot(
      columnarBatch.rootVectors.map(_.fieldVector).toIterable.asJava)
    sibling.setRowCount(sibling.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeSibling(sibling)
  }
}
