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

package com.webank.eggroll.rollframe

import java.util.Random
import java.util.concurrent.{Callable, Executors}

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.io.adapter.{BlockDeviceAdapter, HdfsBlockAdapter}
import com.webank.eggroll.core.meta.{ErPartition, ErProcessor, ErStore, ErStoreLocator}
import com.webank.eggroll.format._
import org.junit.{Before, Test}

/**
 * all unit test run on local mode
 */
class RollFrameTests {
  private val testAssets = TestAssets
  private val clusterManager = testAssets.clusterManager

  @Before
  def setup(): Unit = {
    HdfsBlockAdapter.fastSetLocal()
    clusterManager.setMode("local")
    clusterManager.startServerCluster()
  }

  /**
   * the other test methods depend on these frameBatches, and make sure of configuration of HDFS correctly.
   */
  @Test
  def testCreateFrameBatch(): Unit = {
    val fieldCount = 10
    val rowCount = 100 // total value count = rowCount * fbCount * fieldCount
    val fbCount = 1 // the num of batch

    def write(adapter: FrameDB): Unit = {
      val randomObj = new Random()
      (0 until fbCount).foreach { i =>
        val fb = new FrameBatch(new FrameSchema(testAssets.getDoubleSchema(fieldCount)), rowCount)
        for {x <- 0 until fieldCount
             y <- 0 until rowCount} {
          fb.writeDouble(x, y, randomObj.nextDouble())
          //          fb.writeDouble(x, y, 1)
        }
        println(s"FrameBatch order: $i,row count: ${fb.rowCount}")
        adapter.append(fb)
      }
      adapter.close()
    }

    def writeSplice(adapter: FrameDB): Unit = {
      val sliceFieldCount = fieldCount / 2
      val randomObj = new Random()
      (0 until fbCount).foreach { i =>
        val fb = new FrameBatch(new FrameSchema(testAssets.getDoubleSchema(fieldCount)), rowCount)
        for {x <- 0 until fieldCount
             y <- 0 until rowCount} {
          fb.writeDouble(x, y, randomObj.nextDouble())
        }
        val sliceFb = fb.sliceByColumn(i * sliceFieldCount, (i + 1) * sliceFieldCount)
        println(s"FrameBatch order: $i,row count: ${sliceFb.fieldCount}")
        adapter.append(sliceFb)
      }
      adapter.close()
    }

    def read(adapter: FrameDB): Unit = {
      var num = 0
      adapter.readAll().foreach(_ => num += 1)
      val oneFb = adapter.readOne()
      adapter.close()
      assert(fbCount == num)
      assert(fieldCount == oneFb.fieldCount)
      assert(rowCount == oneFb.rowCount)
    }

    val output = clusterManager.getRollFrameStore("a1", "test1", StringConstants.FILE)

    write(FrameDB(output, 0))
    write(FrameDB(output, 1))
    read(FrameDB(output, 0))
    read(FrameDB(output, 1))

    write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
    write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
    read(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
    read(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
  }

  @Test
  def testNetworkToJvm(): Unit = {
    // 1.write to network
    val networkStore = clusterManager.getRollFrameStore("a1", "test1", StringConstants.NETWORK)
    networkStore.partitions.indices.foreach { i =>
      new Thread() {
        override def run(): Unit = {
          val fbs = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/a1/" + i).readAll()
          FrameDB(networkStore, i).writeAll(fbs)
        }
      }.start()
    }
    // 2.write to cache
    val cacheStore = RollFrame.Util.loadCache(networkStore)
    // 3.assert
    cacheStore.partitions.indices.foreach { i =>
      val fb = FrameDB(cacheStore, i).readOne()
      assert(fb.fieldCount == 10)
      assert(fb.rowCount == 100)
    }
  }

  /**
   * a demo of slice a FrameBatch by rows and broadcast to the cluster to run aggregation
   */
  @Test
  def testSliceByRowAndAggregate(): Unit = {
    // 1. FrameBatch on root server
    val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.FILE)
    val input = ErStore(storeLocator = storeLocator,
      partitions = Array(ErPartition(id = 0, storeLocator = storeLocator, processor = clusterManager.localNode0)))
    val fb = FrameDB(input, 0).readOne()

    // 2. distribute and load to caches
    val networkStore = clusterManager.getRollFrameStore("a1", "test1", StringConstants.NETWORK)
    val partitionsNum = networkStore.partitions.length
    val sliceRowCount = (partitionsNum + fb.rowCount - 1) / partitionsNum
    (0 until partitionsNum).foreach(i => FrameDB(networkStore, i).append(fb.sliceRealByRow(i * sliceRowCount, sliceRowCount)))
    val cacheStore = RollFrame.Util.loadCache(networkStore)
    (0 until partitionsNum).foreach(i => println(s"check id.$i partition's row number: ${FrameDB(cacheStore, i).readOne().rowCount}"))

    // 3. aggregate operation
    val rf = new RollFrameClientMode(cacheStore)
    val start = System.currentTimeMillis()
    val fieldCount = fb.fieldCount
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))
    val outStoreLocator = ErStoreLocator(name = "a1Sr", namespace = "test1", storeType = StringConstants.FILE)
    // TODO: aggregate output Store is inconsistent here
    val outStore = clusterManager.getRollFrameStore("a1Sr", "test1", StringConstants.FILE)
    val outStore1 = ErStore(storeLocator = outStoreLocator, partitions = Array(
      ErPartition(id = 0, storeLocator = outStoreLocator, processor = clusterManager.localNode0)))
    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          val fv = y.rootVectors(f)
          var sum = 0.0
          for (n <- 0 until fv.valueCount) {
            sum += 1
          }
          //          val fv = y.rootVectors(f)
          //          val sum = fv.valueCount
          x.writeDouble(f, 0, sum)
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, output = outStore)
    println(System.currentTimeMillis() - start)
    assert(FrameDB(outStore1, 0).readOne().readDouble(0, 0) == 100)
  }

  @Test
  def testHdfsToJvm(): Unit = {
    val input = clusterManager.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    val output = RollFrame.Util.loadCache(input)
    assert(FrameDB(input, 0).readOne().readDouble(0, 0) == FrameDB(output, 0).readOne().readDouble(0, 0))
    assert(FrameDB(input, 1).readOne().readDouble(0, 0) == FrameDB(output, 1).readOne().readDouble(0, 0))
  }

  @Test
  def testMapBatchWithHdfs(): Unit = {
    val input = clusterManager.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    val output = clusterManager.getRollFrameStore("a1map", "test1", StringConstants.HDFS)

    val rf = new RollFrameClientMode(input)
    rf.mapBatch({ cb =>
      val mapRf = new FrameBatch(cb.rootSchema)
      for (column <- 0 until mapRf.fieldCount) {
        for (row <- 0 until mapRf.rowCount) {
          mapRf.writeDouble(column, row, row)
        }
      }
      mapRf
    }, output = output)

    val mapFb0 = FrameDB(output, 0).readOne() // take first partition to assert
    assert(mapFb0.readDouble(0, 0) == 0.0)
    val mapFb1 = FrameDB(output, 1).readOne() // take second partition to assert
    assert(mapFb1.readDouble(2, 10) == 10.0)
  }

  @Test
  def testAggregateWithHdfs(): Unit = {
    var start = System.currentTimeMillis()
    val inStore = clusterManager.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    // TODO: aggregate output Store is inconsistent here
    val outStore = clusterManager.getRollFrameStore("a1_aggregate", "test1", StringConstants.HDFS)
    val outStoreLocator = ErStoreLocator(name = "a1_aggregate", namespace = "test1", storeType = StringConstants.HDFS)
    val outStore1 = ErStore(storeLocator = outStoreLocator, partitions = Array(
      ErPartition(id = 0, storeLocator = outStoreLocator, processor = clusterManager.localNode0)))

    val rf = new RollFrameClientMode(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          //          val sum = y.rowCount
          val fv = y.rootVectors(f)
          var sum = 0.0
          for (i <- 0 until fv.valueCount) {
            //            sum += fv.readDouble(i)
            sum += 1
          }
          x.writeDouble(f, 0, sum)
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, threadsNum = 2, output = outStore)
    println(System.currentTimeMillis() - start)

    val resultFb = FrameDB(outStore1, 0).readOne()
    assert(resultFb.readDouble(0, 0) == 200)
  }

  def loadCaches(): ErStore = {
    val input = clusterManager.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    RollFrame.Util.loadCache(input)
  }

  @Test
  def testMapBatch(): Unit = {
    val input = loadCaches()
    val output = clusterManager.getRollFrameStore("a1map", "test1", StringConstants.FILE)
    val rf = new RollFrameClientMode(input)
    rf.mapBatch({ cb =>
      val schema =
        """
        {
          "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double2", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}
          ]
        }
      """.stripMargin
      val batch = new FrameBatch(new FrameSchema(schema), 2)
      batch.writeDouble(0, 0, 0.0 + cb.readDouble(0, 0))
      batch.writeDouble(0, 1, 0.1)
      batch.writeDouble(1, 0, 1.0)
      batch.writeDouble(1, 1, 1.1)
      batch
    }, output)

    new RollFrameClientMode(output)
      .mapBatch { fb =>
        assert(fb.readDouble(0, 1) == 0.1)
        assert(fb.readDouble(1, 0) == 1.0)
        assert(fb.readDouble(1, 1) == 1.1)
        fb
      }
  }

  @Test
  def testReduceBatch(): Unit = {
    val clusterManager = new ClusterManager
    val input = clusterManager.getRollFrameStore("a1", "test1")
    val output = clusterManager.getRollFrameStore("a1_reduce", "test1", StringConstants.FILE)
    val rf = new RollFrameClientMode(input)
    rf.reduce({ (x, y) =>
      try {
        for (i <- 0 until x.fieldCount) {
          for (j <- 0 until x.rowCount) {
            x.writeDouble(i, j, x.readDouble(i, j) + y.readDouble(i, j))
          }
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, output = output
    )
    val fb = FrameDB(input, 0).readOne()
    val fb1 = FrameDB(output, 0).readOne()
    assert(fb.fieldCount == fb1.fieldCount)
    assert(fb.rowCount == fb1.rowCount)
  }

  @Test
  def testColumnarWrite1000(): Unit = {
    def pass(path: String): Unit = {
      val fieldCount = 10
      val schema = TestAssets.getDoubleSchema(fieldCount)
      val cw = new FrameWriter(new FrameSchema(schema), BlockDeviceAdapter.file(path))
      val valueCount = 500 * 100 / 2
      val batchSize = 500 * 100 / 2
      cw.write(valueCount, batchSize,
        (fid, cv) => (0 until batchSize).foreach(
          //        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
          n => cv.writeDouble(n, fid * new Random().nextDouble())
        )
      )
      cw.close()
    }

    pass("/tmp/unittests/RollFrameTests/file/test1/b1/0")
    pass("/tmp/unittests/RollFrameTests/file/test1/b1/1")
  }

  @Test
  def testRollFrameAggregateBatch(): Unit = {
    var start = System.currentTimeMillis()
    val inStore = clusterManager.getRollFrameStore("a1", "test1", StringConstants.FILE)
    val rf = new RollFrameClientMode(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          for (i <- 0 until fv.valueCount) {
            //              sum += fv.readDouble(i) / 2
            sum += 1
          }
          x.writeDouble(f, 0, sum)
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    })
    println(System.currentTimeMillis() - start)
    val aggregateFb = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/a1_aggregate/0").readOne()
    assert(aggregateFb.readDouble(0, 0) == 300)
  }

  /**
   * TODO: didn't finished
   */
  @Test
  def testRollFrameAggregateBy(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val inStore = clusterManager.getRollFrameStore("a1", "test1")
    val rf = new RollFrameClientMode(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 3)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          if (fv != null) {
            for (i <- 0 until fv.valueCount) {
              sum += 1
            }
            x.writeDouble(f, 0, sum)
            x.writeDouble(f, 1, sum)
            x.writeDouble(f, 2, sum)
            //          x.writeDouble(f,0, fv.valueCount)
          }

        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        for (j <- 0 until 3) {
          if (b.rootVectors(i) != null) a.writeDouble(i, j, a.readDouble(i, j) + b.readDouble(i, j))
        }
      }
      a
    }, byColumn = true, output = ErStore(ErStoreLocator("file", "test1", "r1byC")))
    println(System.currentTimeMillis() - start)
    val aggregateFb = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/r1byC/0").readOne()
    val aggregateFb1 = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/r1byC/1").readOne()
    println(aggregateFb.readDouble(0, 0))
    println(aggregateFb1.readDouble(5, 0))
  }

  @Test
  def testRollFrameMulti(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val rf = new RollFrameClientMode(clusterManager.getRollFrameStore("a1", "test1"))
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    /*    val output1 = RfStore("r1","test1",1, storeType = FrameDB.CACHE)
        val output2 = RfStore("r2","test1",1, storeType = FrameDB.CACHE)
        val output3 = RfStore("r3","test1",1, storeType = FrameDB.CACHE)*/


    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    val output1 = rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          if (fv != null) {
            for (i <- 0 until fv.valueCount) {
              sum += fv.readDouble(i)
            }
            x.writeDouble(f, 0, sum)
            //          x.writeDouble(f,0, fv.valueCount)
          }
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        if (b.rootVectors(i) != null) a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    })

    val pool = Executors.newFixedThreadPool(2)
    val future1 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        rf.aggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })

    val result1 = future1.get()

    val future2 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        rf.aggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })
    val result2 = future2.get()
    println(System.currentTimeMillis() - start, result1, result2)
  }
}
