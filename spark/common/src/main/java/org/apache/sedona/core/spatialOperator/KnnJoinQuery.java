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
package org.apache.sedona.core.spatialOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.geometryEntities.ExtendedCircle;
import org.apache.sedona.core.geometryEntities.GeometryWithDistance;
import org.apache.sedona.core.knnJoinJudgement.KnnJoinJudgement;
import org.apache.sedona.core.knnJoinJudgement.KnnJoinJudgementUsingIndex;
import org.apache.sedona.core.knnJoinJudgement.KnnRangeJoinJudgement;
import org.apache.sedona.core.knnJoinJudgement.KnnRangeJoinJudgementUsingIndex;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;
import scala.Tuple3;

/** @ClassName:KnnJoinQuery @Description:TODO @Author:yolo @Date:2024/10/914:23 @Version:1.0 */
public class KnnJoinQuery {
  public static <U extends Geometry>
      JavaPairRDD<U, List<GeometryWithDistance>> KNNJoinQueryWithDistance(
          SpatialRDD<U> spatialRDD, int k, boolean useIndex) throws Exception {
    final JavaPairRDD<U, List<GeometryWithDistance>> joinResults =
        KNNJoinWithDistance(spatialRDD, k, useIndex);
    return joinResults;
  }

  public static <U extends Geometry> JavaPairRDD<U, List<GeometryWithDistance>> KNNJoinWithDistance(
      SpatialRDD<U> spatialRDD, int k, boolean useIndex) throws Exception {

    JavaRDD<Pair<U, List<GeometryWithDistance>>> firstRoundKNNJoin;
    long t3;
    // 1. build index and first round KnnJoin----------
    if (useIndex) {
      long t1 = System.currentTimeMillis();
      spatialRDD.buildIndex(IndexType.EXTENDED_RTREE, true);
      long t2 = System.currentTimeMillis();
      System.out.println("build index on partitioned SRDD time:" + (t2 - t1) / 1000.0);
      t3 = System.currentTimeMillis();
      firstRoundKNNJoin =
          spatialRDD.spatialPartitionedRDD.zipPartitions(
              spatialRDD.indexedRDD, new KnnJoinJudgementUsingIndex(k));
    } else {
      t3 = System.currentTimeMillis();
      firstRoundKNNJoin =
          spatialRDD.spatialPartitionedRDD.zipPartitions(
              spatialRDD.spatialPartitionedRDD, new KnnJoinJudgement(k));
    }
    // System.out.println("firstRoundKNNJoin:" + firstRoundKNNJoin.count());

    // 2. calculate circle and overlapPartition id
    JavaRDD<Pair<U, Tuple3<List<GeometryWithDistance>, HashSet<Integer>, ExtendedCircle>>>
        withCircleandOverlaps =
            getWithCircleandOverlaps(useIndex, spatialRDD.getPartitioner(), firstRoundKNNJoin)
                .persist(StorageLevel.MEMORY_ONLY());
    //        JavaRDD<Pair<U, Tuple3<List<GeometryWithDistance>, HashSet<Integer>, ExtendedCircle>>>
    // withCircleandOverlaps = getWithFilteredCircleandOverlaps(useIndex, firstRoundKNNJoin,
    // spatialRDD).persist(StorageLevel.MEMORY_ONLY());
    // System.out.println(withCircleandOverlaps.collect());

    // 3. filter nonFinalKnn geometry
    JavaRDD<Pair<U, Tuple3<List<GeometryWithDistance>, HashSet<Integer>, ExtendedCircle>>>
        nonFinalKnnLists =
            withCircleandOverlaps.filter(
                pair -> pair.getValue()._2().size() > 0 && pair.getValue()._3().getRadius() > 0);
    //        System.out.println("nonFinalKnnLists: "+nonFinalKnnLists.count());

    // 4. repartition circle to overlapPartition
    JavaPairRDD<
            Integer, Pair<U, Tuple3<List<GeometryWithDistance>, HashSet<Integer>, ExtendedCircle>>>
        overlapKnnLists =
            nonFinalKnnLists.flatMapToPair(
                pair -> {
                  List<
                          Tuple2<
                              Integer,
                              Pair<
                                  U,
                                  Tuple3<
                                      List<GeometryWithDistance>,
                                      HashSet<Integer>,
                                      ExtendedCircle>>>>
                      keyValuePairs = new ArrayList<>();
                  HashSet<Integer> overlapSet = pair.getValue()._2();
                  for (Integer value : overlapSet) {
                    keyValuePairs.add(new Tuple2<>(value, pair));
                  }
                  return keyValuePairs.iterator();
                });
    //        System.out.println("spatialRDDPartitions: " +
    // spatialRDD.getPartitioner().numPartitions());
    Partitioner partitioner =
        new Partitioner() {
          @Override
          public int numPartitions() {
            return spatialRDD.getPartitioner().numPartitions();
          }

          @Override
          public int getPartition(Object key) {
            return (Integer) key;
          }
        };
    JavaRDD<ExtendedCircle> overlapCircles =
        overlapKnnLists.partitionBy(partitioner).map(pair -> pair._2().getValue()._3());
    //        System.out.println("overlapCircles: "+overlapCircles.count());

    // 5. second round AllKnnJoin on overlap partition based on search circle
    JavaPairRDD<Integer, Tuple2<U, List<GeometryWithDistance>>> overlapCircleKnnLists = null;
    if (useIndex) {
      overlapCircleKnnLists =
          overlapCircles
              .zipPartitions(spatialRDD.indexedRDD, new KnnRangeJoinJudgementUsingIndex(k))
              .mapToPair(
                  (PairFunction<
                          Pair<Integer, Tuple2<U, List<GeometryWithDistance>>>,
                          Integer,
                          Tuple2<U, List<GeometryWithDistance>>>)
                      pair -> new Tuple2<>(pair.getKey(), pair.getValue()));
    } else {
      overlapCircleKnnLists =
          overlapCircles
              .zipPartitions(spatialRDD.spatialPartitionedRDD, new KnnRangeJoinJudgement(k))
              .mapToPair(
                  (PairFunction<
                          Pair<Integer, Tuple2<U, List<GeometryWithDistance>>>,
                          Integer,
                          Tuple2<U, List<GeometryWithDistance>>>)
                      pair -> new Tuple2<>(pair.getKey(), pair.getValue()));
    }
    //        System.out.println("overlapCircleKnnPartitions: " +
    // overlapCircleKnnLists.getNumPartitions());

    // 6. union nonFinalKnnLists
    JavaPairRDD<Integer, Tuple2<U, List<GeometryWithDistance>>> firstRoundResult =
        withCircleandOverlaps
            .mapToPair(
                pair ->
                    new Tuple2<>(
                        pair.getRight()._3().getPartitionId(),
                        new Tuple2<>(pair.getLeft(), pair.getRight()._1())))
            .partitionBy(partitioner);
    JavaPairRDD<Integer, Tuple2<U, List<GeometryWithDistance>>> secondRoundResult =
        overlapCircleKnnLists.partitionBy(partitioner);
    JavaPairRDD<U, List<GeometryWithDistance>> result =
        firstRoundResult
            .union(secondRoundResult)
            .mapToPair(pair -> new Tuple2<>(pair._2._1, pair._2._2))
            .reduceByKey(
                (list1, list2) -> {
                  //                    System.out.println("list1:"+list1);
                  //                    System.out.println("list2:"+list2);
                  return unionKnnList(k, list1, list2);
                });
    System.out.println(result.getNumPartitions());

    withCircleandOverlaps.unpersist();
    long t4 = System.currentTimeMillis();
    System.out.println("all knn join time:" + (t4 - t3) / 1000.0);

    //      System.out.println("results: "+result.count());

    return result;
  }

  // TODO 去重
  private static List<GeometryWithDistance> unionKnnList(
      int k, List<GeometryWithDistance> list1, List<GeometryWithDistance> list2) {
    List<GeometryWithDistance> mergedList = new ArrayList<>();
    if (list1.isEmpty()) return list2;
    else if (list2.isEmpty()) return list1;
    int i = 0;
    int j = 0;
    HashSet<Long> idSet = new HashSet<>();
    while (mergedList.size() < k && i < list1.size() && j < list2.size()) {
      if (list1.get(i).getDistance() < list2.get(j).getDistance()) {
        if (!idSet.contains(list1.get(i).getIndexID())) {
          idSet.add(list1.get(i).getIndexID());
          mergedList.add(list1.get(i));
        }
        i++;
      } else {
        if (!idSet.contains(list2.get(j).getIndexID())) {
          idSet.add(list2.get(j).getIndexID());
          mergedList.add(list2.get(j));
        }
        j++;
      }
    }
    while (mergedList.size() < k && i < list1.size()) {
      if (!idSet.contains(list1.get(i).getIndexID())) {
        idSet.add(list1.get(i).getIndexID());
        mergedList.add(list1.get(i));
      }
      i++;
    }
    while (mergedList.size() < k && j < list2.size()) {
      if (!idSet.contains(list2.get(j).getIndexID())) {
        idSet.add(list2.get(j).getIndexID());
        mergedList.add(list2.get(j));
      }
      j++;
    }
    return mergedList;
  }

  private static <U extends Geometry>
      JavaRDD<Pair<U, Tuple3<List<GeometryWithDistance>, HashSet<Integer>, ExtendedCircle>>>
          getWithCircleandOverlaps(
              boolean useIndex,
              SpatialPartitioner partitioner,
              JavaRDD<Pair<U, List<GeometryWithDistance>>> firstRoundKNNJoin) {
    return firstRoundKNNJoin.map(
        pair -> {
          final List<GeometryWithDistance> knnList = pair.getValue();
          if (knnList.size() == 1) {
            return Pair.of(
                pair.getKey(),
                new Tuple3<>(knnList, new HashSet<>(), new ExtendedCircle(pair.getKey(), 0.0)));
          }
          //            System.out.println(knnList);
          final double maxDistance =
              useIndex
                  ? knnList.get(knnList.size() - 1).getDistance()
                  : knnList.get(0).getDistance();

          final ExtendedCircle circle = new ExtendedCircle(pair.getKey(), maxDistance);

          Iterator<Tuple2<Integer, ExtendedCircle>> overlapCircles =
              partitioner.placeObject(circle);

          HashSet<Integer> overlapSet = new HashSet<Integer>();
          while (overlapCircles.hasNext()) {
            Tuple2<Integer, ExtendedCircle> next = overlapCircles.next();
            if (next._1 != TaskContext.getPartitionId()) overlapSet.add(next._1);
            else circle.setPartitionId(next._1);
          }
          //            System.out.println("PartitionId: "+circle.getPartitionId());
          return Pair.of(pair.getKey(), new Tuple3<>(knnList, overlapSet, circle));
        });
  }

  private static <U extends Geometry>
      JavaRDD<Pair<U, Tuple3<List<GeometryWithDistance>, HashSet<Integer>, ExtendedCircle>>>
          getWithFilteredCircleandOverlaps(
              boolean useIndex,
              JavaRDD<Pair<U, List<GeometryWithDistance>>> firstRoundKNNJoin,
              SpatialRDD<U> spatialRDD) {
    List<Tuple2<Integer, Envelope>> strTreeRegions =
        spatialRDD
            .indexedRDD
            .mapPartitionsWithIndex(
                (partitionId, iterator) -> {
                  List<Tuple2<Integer, Envelope>> results = new ArrayList<>();
                  while (iterator.hasNext()) {
                    STRtree spatialIndex = (STRtree) iterator.next();
                    Envelope envelope = (Envelope) spatialIndex.getRoot().getBounds();
                    results.add(new Tuple2<>(partitionId, envelope));
                  }
                  return results.iterator();
                },
                true)
            .collect();

    return firstRoundKNNJoin.map(
        pair -> {
          final List<GeometryWithDistance> knnList = pair.getValue();
          if (knnList.size() == 0) {
            return Pair.of(
                pair.getKey(),
                new Tuple3<>(knnList, new HashSet<>(), new ExtendedCircle(pair.getKey(), 0.0)));
          }

          final double maxDistance =
              useIndex
                  ? knnList.get(knnList.size() - 1).getDistance()
                  : knnList.get(0).getDistance();

          final ExtendedCircle circle = new ExtendedCircle(pair.getKey(), maxDistance);

          HashSet<Integer> overlapSet = new HashSet<Integer>();
          for (Tuple2<Integer, Envelope> region : strTreeRegions) {
            if (region._2 == null) continue;
            if (TaskContext.getPartitionId() == region._1) {
              circle.setPartitionId(region._1);
              continue;
            }
            if (!region._2.disjoint(circle.getEnvelopeInternal())) {
              overlapSet.add(region._1);
            }
          }
          //            System.out.println("PartitionId: "+circle.getPartitionId());
          return Pair.of(pair.getKey(), new Tuple3<>(knnList, overlapSet, circle));
        });
  }
}
