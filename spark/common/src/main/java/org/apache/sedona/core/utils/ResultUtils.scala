package org.apache.sedona.core.utils

import org.apache.sedona.common.geometryObjects.Circle
import org.apache.sedona.core.geometryEntities.{CDCPoint, GeometryWithDistance}
import org.apache.sedona.core.spatialRDD.PointRDD
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.locationtech.jts.geom.{Envelope, Geometry, Point}
import org.locationtech.jts.index.SpatialIndex
import org.locationtech.jts.index.strtree.STRtree

import java.io.{File, FileWriter}
import java.util
import java.util.stream.Collectors
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ResultUtils {
  def outputPartitionAndGrid(spatialRDD: PointRDD, pPath: String, gPath: String): Unit = {
    IOUtils.createFile(pPath)
    IOUtils.createFile(gPath)
    val pointsList: util.List[(Int, Point)] = spatialRDD.spatialPartitionedRDD.map(point => (TaskContext.getPartitionId(), point)).collect()
    val gridsList: util.List[Envelope] = spatialRDD.getPartitioner.getGrids
    val numPartitions: Int = spatialRDD.getPartitioner.getPartition(2)
    //    问题：空分区
    //    val partitionSize: Array[(Int, Int)] = spatialRDD.spatialPartitionedRDD.map(point => (TaskContext.getPartitionId(), 1)).rdd
    //      .groupBy(_._1) // 按分区 ID 进行分组
    //      .mapValues(_.size)// 统计每个分区的元素个数
    //      .collect()
    val partitionSize: Array[(Int, Int)] = spatialRDD.spatialPartitionedRDD.rdd.mapPartitions(iterator => {
      val partitionName = TaskContext.getPartitionId() // Get the name of the current partition
      var size = 0
      while (iterator.hasNext) {
        iterator.next()
        size += 1
      }
      Iterator((partitionName, size))
    }, preservesPartitioning = true).collect()

    val pwriter = new FileWriter(new File(pPath))
    pointsList.forEach {
      case (partitionId, point) =>
        val split = (point.getUserData.toString).split(",|\\s+|;")
        val pointId = split(1)
        val label = split(0)
        //        val pointId = split(split.length - 3)
        val row = s"${point.getCentroid.getX},${point.getCentroid.getY},$label,$pointId,$partitionId"
        pwriter.write(row + "\n") // 写入 CSV 行
    }

    val gwriter = new FileWriter(new File(gPath))
    for (i <- 0 until gridsList.size()) {
      val grid: Envelope = gridsList.get(i)
      val psize: (Int, Int) = partitionSize(i)
      val row = s"${grid.getMinX},${grid.getMaxX},${grid.getMinY},${grid.getMaxY},${psize._1},${psize._2}"
      gwriter.write(row + "\n")
    }
    pwriter.close()
    gwriter.close()
  }

  def outputSTRTreeNode(spatialRDD: PointRDD, nPath: String): Unit = {
    IOUtils.createFile(nPath)
    val boundList: util.List[Envelope] = spatialRDD.indexedRDD.map((index: SpatialIndex) => {
      val strIndex: STRtree = index.asInstanceOf[STRtree]
      strIndex.getRoot.getBounds.asInstanceOf[Envelope]
    }).collect()
    val nwriter: FileWriter = new FileWriter(new File(nPath))
    for (i <- 0 until boundList.size()) {
      val grid: Envelope = boundList.get(i)
      if (grid != null) {
        val row: String = s"${grid.getMinX},${grid.getMaxX},${grid.getMinY},${grid.getMaxY}"
        nwriter.write(row + "\n")
      }
    }
    nwriter.close()
  }

  def outputOverlapCircle(overlapCircles: util.List[Circle], k: Int) = {
    val formatter = DateTimeFormat.forPattern("MMdd")
    val time = formatter.print(LocalDate.now())
    val cPath: String = System.getProperty("user.dir") + "/src/test/resources" + "/ResultData/KNNResult/QGCDC/" + time + "/" + k + "_circle.csv"
    IOUtils.createFile(cPath)
    val cwriter: FileWriter = new FileWriter(new File(cPath))
    for (i <- 0 until overlapCircles.size()) {
      val circle: Circle = overlapCircles.get(i)
      val row: String = s"${circle.getCenterPoint.x},${circle.getCenterPoint.y},${circle.getRadius}"
      cwriter.write(row + "\n")
    }
    cwriter.close()
  }


  def outputKnnResult(knnResult: Array[(Point, util.List[GeometryWithDistance[_ <: Geometry]])], trueResult: Array[(Point, util.List[GeometryWithDistance[_ <: Geometry]])], kPath: String, tPath: String) = {
    IOUtils.createFile(kPath)
    IOUtils.createFile(tPath)
    val kwriter: FileWriter = new FileWriter(new File(kPath))
    val twriter: FileWriter = new FileWriter(new File(tPath))

    val knnIdMap: Map[String, Set[Long]] = knnResult.map { case (point, knnList) =>
      val knnSet = knnList.map(geom => geom.getIndexID).toSet
      val userData: Array[String] = point.getUserData.toString.split(",|\\s+|;")
      (userData(1), knnSet)
    }.toMap

    knnResult.foreach { case (point, knnList) =>
      val userData: Array[String] = point.getUserData.toString.split(",|\\s+|;")
      knnList.foreach {
        geom =>
          val row = s"${userData(1)},${geom.getIndexID},${geom.getDistance}"
          kwriter.write(row + "\n")
      }
    }

    val trueIdMap: Map[String, Set[Long]] = trueResult.map { case (point, knnList) =>
      val knnSet = knnList.map(geom => geom.getIndexID).toSet
      val userData: Array[String] = point.getUserData.toString.split(",|\\s+|;")
      (userData(1), knnSet)
    }.toMap

    trueResult.foreach { case (point, knnList) =>
      val userData: Array[String] = point.getUserData.toString.split(",|\\s+|;")
      knnList.foreach {
        geom =>
          val row = s"${userData(1)},${geom.getIndexID},${geom.getDistance}"
          twriter.write(row + "\n")
      }
    }

    val intersectionSizes = knnIdMap.keys.view
      .filter(trueIdMap.contains)
      .map(key => {
        val knnSet = knnIdMap(key)
        val trueSet = trueIdMap(key)
        knnSet.intersect(trueSet).size
      })

    val totalIntersectionSize = intersectionSizes.sum
    val totalSize = trueResult.map {case (point, knnList) =>
      knnList.size()
    }.sum

    println("accuracy:" + totalIntersectionSize.toDouble / totalSize)

    kwriter.close()
    twriter.close()
  }

  def outputKnnResult2(knnResult: util.List[(Point, util.List[Point])], trueResult: Array[(Point, util.List[GeometryWithDistance[_ <: Geometry]])], kPath: String, tPath: String) = {
    IOUtils.createFile(kPath)
    IOUtils.createFile(tPath)
    val kwriter: FileWriter = new FileWriter(new File(kPath))
    val twriter: FileWriter = new FileWriter(new File(tPath))

    val knnIdMap: Map[String, Set[String]] = knnResult.map { case (point, knnList) =>
      val knnSet = knnList.map(geom => geom.getUserData.toString.split(",|\\s+|;")(1)).toSet
      val userData: Array[String] = point.getUserData.toString.split(",|\\s+|;")
      (userData(1), knnSet)
    }.toMap

    val trueIdMap: Map[String, Set[String]] = trueResult.map { case (point, knnList) =>
      val knnSet = knnList.map(geom => geom.getIndexID.toString).toSet
      val userData: Array[String] = point.getUserData.toString.split(",|\\s+|;")
      (userData(1), knnSet)
    }.toMap

    val intersectionSizes = knnIdMap.keys.view
      .filter(trueIdMap.contains)
      .map(key => {
        val knnSet = knnIdMap(key)
        val trueSet = trueIdMap(key)
        knnSet.intersect(trueSet).size
      })

    val totalIntersectionSize = intersectionSizes.sum
    val totalSize = trueResult.map {case (point, knnList) =>
      knnList.size()
    }.sum

    println("accuracy:" + totalIntersectionSize.toDouble / totalSize)

    kwriter.close()
    twriter.close()
  }
}
