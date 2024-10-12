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
package org.apache.sedona.core.knnJoinJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.geometryEntities.GeometryWithDistance;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;

/** @ClassName:KnnJoinJudgement @Description:TODO @Author:yolo @Date:2024/10/910:27 @Version:1.0 */
public class KnnJoinJudgement<U extends Geometry>
    implements FlatMapFunction2<Iterator<U>, Iterator<U>, Pair<U, List<GeometryWithDistance>>>,
        Serializable {

  private final int k;

  public KnnJoinJudgement(int k) {
    this.k = k;
  }

  @Override
  public Iterator<Pair<U, List<GeometryWithDistance>>> call(
      Iterator<U> pointIterator, Iterator<U> searchIterator) throws Exception {
    List<Pair<U, List<GeometryWithDistance>>> result = new ArrayList<>();
    List<U> searchPoints = new ArrayList<>();
    while (searchIterator.hasNext()) {
      searchPoints.add(searchIterator.next());
    }

    while (pointIterator.hasNext()) {
      U queryPoint = pointIterator.next();
      PriorityQueue<GeometryWithDistance> pq =
          new PriorityQueue<GeometryWithDistance>(k, new GeometryDistanceComparator(false));
      for (int i = 0; i < searchPoints.size(); i++) {
        U searchPoint = searchPoints.get(i);
        double distance = searchPoint.distance(queryPoint);
        if (pq.size() < k) {
          pq.offer(new GeometryWithDistance(searchPoint, distance));
        } else {
          double largestDistanceInPriQueue = pq.peek().getGeometry().distance(queryPoint);
          if (largestDistanceInPriQueue > distance) {
            pq.poll();
            pq.offer(new GeometryWithDistance(searchPoint, distance));
          }
        }
      }
      ArrayList<GeometryWithDistance> res = new ArrayList<GeometryWithDistance>();
      if (pq.size() >= k) {
        for (int j = 0; j < k; j++) {
          res.add(pq.poll());
        }
      } else {
        for (int j = 0; j < pq.size(); j++) {
          res.add(pq.poll());
        }
      }
      //            System.out.println("knn:"+res);
      result.add(Pair.of(queryPoint, res));
    }

    return result.iterator();
  }
}
