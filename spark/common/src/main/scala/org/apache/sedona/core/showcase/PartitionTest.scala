/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.showcase

import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{DistanceMetric, GridType, IndexType}
import org.apache.sedona.core.knnJoinJudgement.KnnJoinJudgement
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialOperator.{JoinQuery, KnnJoinQuery}
import org.apache.sedona.core.spatialRDD.PointRDD
import org.apache.sedona.core.utils.ResultUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.locationtech.jts.geom.Point

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object PartitionTest {
  var sc: SparkContext = null

  var dataInputLocation: String = null

  var clusterResultOutputLocation: String = null

  var KNNResultOutputLocation: String = null

  var circleResultOutPutLocation: String = null

  var partitionResultOutPutLocation: String = null

  var clusterEvaluationOutPutLocation: String = null

  var dataFileName: String = null

  var dataFileType: String = null

  var numPartitions: Int = 0

  var realNumPartitions: Int = 0

  var useIndex: Boolean = true

  var indexType: IndexType = null

  var gridType: GridType = null

  var k: Int = 31

  var dcmRatio: Double = 0.9

  def main(args: Array[String]): Unit = {
    initailize()
    //    val pointRDD = readData()
    //    partition(pointRDD)
    //    allKnnJoin(pointRDD)
    sc.stop()
  }

  def initailize(): Unit = {
    // 设置提交任务的用户
    //    System.setProperty("HADOOP_USER_NAME", "root")
    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources"
    //    val resourceFolder = "file:///vda/cdc"

    // 初始化环境
    val conf = new SparkConf()
      .setAppName("QGCDC_Sedona")
      .setMaster("spark://10.101.242.200:7077")
    //      .set("spark.driver.host", "10.101.61.12")
    //      .setJars(List("E:\\Projects\\jars\\PartitionTest-1.0-SNAPSHOT.jar"))
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    //          conf.setMaster("spark://Master:7077")
    conf
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.max", "512M")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    //      .set("spark.driver.memory", "8G")
    //      .set("spark.executor.memory", "4G")
//      .set("spark.executor.instances", "8")
//      .set("spark.executor.cores", "16")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession = spark.getOrCreate()
    sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    indexType = IndexType.RTREE
    //    gridType = NewGridType.QUADTREE
    //    gridType = NewGridType.QUADTREEWF
//    val sparkExecCores: Integer = conf.get("spark.executor.cores").toInt
//    val sparkNumExec: Integer = conf.get("spark.executor.instances").toInt
    //    val sparkNumExec: Int = sc.getExecutorMemoryStatus.size
//    System.out.println("sparkNumExec: " + sparkNumExec)
//    val countAllCores: Int = sparkExecCores * sparkNumExec
    numPartitions = 16
    //    numPartitions = countAllCores
    dataFileType = "TXT2"
    dataFileType match {
      case "TXT1" => {
        //        for (i <- Range(1, 7)) {
        for (i <- List(1, 2)) {
          //          var dataFileName = "NDS" + i
          dataFileName = "DS" + i
          for (gType <- List(
              GridType.QUADTREE,
              GridType.ImprovedQT,
//              GridType.QUADTREE_RTREE,
              GridType.KDBTREE,
//              GridType.Voronoi,
              GridType.STRTREE
//              GridType.Hilbert
            )) {
            //          for (gType <- List(GridType.QUADTREE_RTREE)) {
            gridType = gType
            //          for (m <- Range(5, 31, 2)) {
            //            for (m <- Range(5, 61, 5)) {
            //            k = m
            k = 21
            //            for (n <- Range(50, 96, 5)) {
            dcmRatio = 70 / 100.0
            //              dcmRatio = n / 100.0
            println(
              "文件名:" + dataFileName + "\t分区数:" + numPartitions + "\t分区类型:" + gridType + "\t邻居数:" + k + "\tDCM阈值比例:" + dcmRatio)
            //  val resourceFolder: String = "file:///vda/cdc"
            val filePrefix =
              "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio
            val formatter = DateTimeFormat.forPattern("MMdd")
            val time = formatter.print(LocalDate.now())
            println("time" + time)
            //            dataInputLocation = resourceFolder + "/data/SyntheticDatasets/" + dataFileName + "_new.csv"
            //            clusterResultOutputLocation = resourceFolder + "/ResultData/clusterResult/QGCDC/" + time + filePrefix + ".txt"
            //            KNNResultOutputLocation = resourceFolder + "/ResultData/KNNResult/QGCDC/" + time + filePrefix
            //            partitionResultOutPutLocation = resourceFolder + "/ResultData/partitionResult/QGCDC/" + time + filePrefix
            val inputResourceFolder: String = "hdfs://Master:9000/data/CDC"
            val outputResourceFolder: String = "/spark/CDC/result"
            dataInputLocation =
              inputResourceFolder + "/SyntheticDatasets/" + dataFileName + "_new.csv"
            clusterResultOutputLocation =
              outputResourceFolder + "/clusterResult/QGCDC/" + time + filePrefix + ".txt"
            KNNResultOutputLocation =
              outputResourceFolder + "/KNNResult/QGCDC/" + time + filePrefix
            partitionResultOutPutLocation =
              outputResourceFolder + "/partitionResult/QGCDC/" + time + filePrefix
            process()
          }
        }
        //        }
      }
      case "TXT2" => {
        for (i <- List("Levine", "Samusik")) {
          var dataFileName = i + "_UMAP"
          for (gType <- List(
              GridType.QUADTREE,
              GridType.ImprovedQT,
//              GridType.QUADTREE_RTREE,
              GridType.KDBTREE,
//              GridType.Voronoi,
              GridType.STRTREE
//              GridType.Hilbert
            )) {
            //          for (gType <- List(GridType.ImprovedQT)) {
            gridType = gType
            k = 50
            dcmRatio = 0.7
            //          for (m <- Range(30, 71, 10)) {
            //            var k = m
            //            for (n <- Range(50, 96, 5)) {
            //              var dcmRatio = n / 100.0
            println(
              "文件名:" + dataFileName + "\t分区数:" + numPartitions + "\t分区类型:" + gridType + "\t邻居数:" + k + "\tDCM阈值比例:" + dcmRatio)
            val filePrefix =
              "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio
            val formatter = DateTimeFormat.forPattern("MMdd")
            val time = formatter.print(LocalDate.now())
            println("time" + time)
            //            dataInputLocation = resourceFolder + "/data/SyntheticDatasets/" + dataFileName + "_new.csv"
            //            clusterResultOutputLocation = resourceFolder + "/ResultData/clusterResult/QGCDC/" + time + filePrefix + ".txt"
            //            KNNResultOutputLocation = resourceFolder + "/ResultData/KNNResult/QGCDC/" + time + filePrefix
            //            partitionResultOutPutLocation = resourceFolder + "/ResultData/partitionResult/QGCDC/" + time + filePrefix
            val inputResourceFolder: String = "hdfs://Master:9000/data/CDC"
            val outputResourceFolder: String = "/spark/CDC/result"
            dataInputLocation =
              inputResourceFolder + "/SyntheticDatasets/" + dataFileName + "_new.csv"
            clusterResultOutputLocation =
              outputResourceFolder + "/clusterResult/QGCDC/" + time + filePrefix + ".txt"
            KNNResultOutputLocation =
              outputResourceFolder + "/KNNResult/QGCDC/" + time + filePrefix
            partitionResultOutPutLocation =
              outputResourceFolder + "/partitionResult/QGCDC/" + time + filePrefix
            process()
            //            }
          }
        }
      }
      case "CSV" => {
        //        for (i <- List(10000, 20000, 50000, 100000, 200000, 300000)) {
        for (i <- List(300000)) {
          var dataFileName = "hubei-" + i
          //          for (m <- Range((math.ceil(math.log(i) / math.log(2)) + 10).toInt, 5 * (math.ceil(math.log(i) / math.log(2))).toInt + 1, 5)) {
          //            var k = m
          var k = (math.ceil(math.log(i) / math.log(2)) + 10).toInt
          //              var k=5*(math.ceil(math.log(i)/math.log(2))).toInt
          //            for (n <- Range(70, 96, 5)) {
          //              var dcmRatio = n / 100.0
          var dcmRatio = 70 / 100.0
          println(
            "文件名:" + dataFileName + "\t分区数:" + numPartitions + "\t分区类型:" + gridType + "\t邻居数:" + k + "\tDCM阈值比例:" + dcmRatio)
          val filePrefix =
            "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio
          val formatter = DateTimeFormat.forPattern("MMdd")
          val time = formatter.print(LocalDate.now())
          println("time" + time)
          dataInputLocation =
            resourceFolder + "/data/GeoDataSets/0.37 Million Enterprise Registration Data in Hubei Province/Points/" + dataFileName + "-new2.csv"
          clusterResultOutputLocation =
            resourceFolder + "/ResultData/clusterResult/QGCDC/" + time + filePrefix + ".txt"
          KNNResultOutputLocation =
            resourceFolder + "/ResultData/KNNResult/QGCDC/" + time + filePrefix + ".txt"
          partitionResultOutPutLocation =
            resourceFolder + "/ResultData/partitionResult/QGCDC/" + time + filePrefix
          //                        val inputResourceFolder: String = "hdfs://Master:9000/data/"
          //                        val outputResourceFolder: String = "/spark/CDC/result"
          //                        dataInputLocation = inputResourceFolder + dataFileName + "-new2.csv"
          //                        clusterResultOutputLocation = outputResourceFolder + "/clusterResult/QGCDC_Sedona/test0114/" + dataFileName + "/" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio + ".txt"
          //                        clusterEvaluationOutPutLocation = outputResourceFolder + "/clusterEvaluation/QGCDC_Sedona/test0114.txt"
        }
      }
      //        }
      //      }
    }
  }

  def process() = {
    val pointRDD = readData()
    partition(pointRDD)
    knnJoin2(pointRDD)
  }

  def readData() = {
    // read data
    println("读取数据开始----------")
    val readDataStart: Long = System.currentTimeMillis()
    val spatialRDD: PointRDD =
      new PointRDD(sc, dataInputLocation, 0, FileDataSplitter.CSV, true, numPartitions)
    val readDataEnd: Long = System.currentTimeMillis()
    println("读取数据结束----------")
    println("时间：" + (readDataEnd - readDataStart) / 1000.0 + "秒")
    spatialRDD
  }

  def partition(spatialRDD: PointRDD) = {
    // partition data
    println("空间分区开始----------")
    val partitionStart: Long = System.currentTimeMillis()
    spatialRDD.setNeighborSampleNumber(k)
    spatialRDD.spatialPartitioning(gridType, numPartitions, false)
    val partitionEnd: Long = System.currentTimeMillis()
    println("空间分区结束----------")
    println("时间：" + (partitionEnd - partitionStart) / 1000.0 + "秒")
    //    ResultUtils.outputPartitionAndGrid(spatialRDD, partitionResultOutPutLocation + "_partition.csv", partitionResultOutPutLocation + "_grid.csv");
  }

  def knnJoin(spatialRDD: PointRDD) = {
    // knn join query
    println("近邻搜索开始----------")
    val allKNNJoinStart: Long = System.currentTimeMillis()
    val knnRDD = KnnJoinQuery.KNNJoinQueryWithDistance(spatialRDD, k, useIndex).rdd
    val knnResult = knnRDD.collect()
    val allKNNJoinEnd: Long = System.currentTimeMillis()
    println("近邻搜索结束----------")
    println("时间：" + (allKNNJoinEnd - allKNNJoinStart) / 1000.0 + "秒")
//    val data = spatialRDD.rawSpatialRDD.collect()
//    val judgement = new KnnJoinJudgement[Point](k)
//    val trueKnnResult = judgement.call(data.iterator, data.iterator).asScala.map(pair => (pair.getLeft, pair.getRight)).toArray
//    println("knnRDDPartitions: " + knnRDD.getNumPartitions)
//    //    ResultUtils.outputSTRTreeNode(spatialRDD, partitionResultOutPutLocation + "_nodeBound.csv")
//    ResultUtils.outputKnnResult(knnResult, trueKnnResult, KNNResultOutputLocation + "_knn.csv", KNNResultOutputLocation + "_trueKnn.csv")
  }

  def knnJoin2(spatialRDD: PointRDD) = {
    println("近邻搜索开始----------")
    val allKNNJoinStart: Long = System.currentTimeMillis()
    spatialRDD.buildIndex(IndexType.RTREE, true)
    val knnRDD =
      JoinQuery.KNNJoinQuery(spatialRDD, spatialRDD, IndexType.RTREE, k, DistanceMetric.EUCLIDEAN)
    val knnResult = knnRDD.collect()
    val allKNNJoinEnd: Long = System.currentTimeMillis()
    println("近邻搜索结束----------")
    println("时间：" + (allKNNJoinEnd - allKNNJoinStart) / 1000.0 + "秒")
//    val data = spatialRDD.rawSpatialRDD.collect()
//    val judgement = new KnnJoinJudgement[Point](k)
//    val trueKnnResult = judgement
//      .call(data.iterator, data.iterator)
//      .asScala
//      .map(pair => (pair.getLeft, pair.getRight))
//      .toArray
//    println("knnRDDPartitions: " + knnRDD.getNumPartitions)
//    //    ResultUtils.outputSTRTreeNode(spatialRDD, partitionResultOutPutLocation + "_nodeBound.csv")
//    ResultUtils.outputKnnResult2(
//      knnResult,
//      trueKnnResult,
//      KNNResultOutputLocation + "_knn.csv",
//      KNNResultOutputLocation + "_trueKnn.csv")

  }

}
