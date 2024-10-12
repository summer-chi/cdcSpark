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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.geometryEntities.ExtendedCircle;
import org.apache.sedona.core.geometryEntities.GeometryWithDistance;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.ExtendedSTRtree;
import org.locationtech.jts.index.strtree.GeometryItemDistance;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

/**
 * @ClassName:KnnRangeJoinJudgementUsingIndex @Description:TODO @Author:yolo @Date:2024/10/910:28 @Version:1.0
 */
public class KnnRangeJoinJudgementUsingIndex<U extends Geometry>
    implements FlatMapFunction2<
            Iterator<ExtendedCircle>,
            Iterator<SpatialIndex>,
            Pair<Integer, Tuple2<U, List<GeometryWithDistance>>>>,
        Serializable {
  private final int k;

  public KnnRangeJoinJudgementUsingIndex(int k) {
    this.k = k;
  }

  @Override
  public Iterator<Pair<Integer, Tuple2<U, List<GeometryWithDistance>>>> call(
      Iterator<ExtendedCircle> circles, Iterator<SpatialIndex> indexIterator) throws Exception {
    List<Pair<Integer, Tuple2<U, List<GeometryWithDistance>>>> result = new ArrayList<>();

    if (!indexIterator.hasNext() || !circles.hasNext()) {
      return result.iterator();
    }

    GeometryItemDistance geometryItemDistance = new GeometryItemDistance();
    SpatialIndex treeIndex = indexIterator.next();
    if (treeIndex instanceof STRtree) {
      if (((ExtendedSTRtree) treeIndex).getRoot().isEmpty()) {
        return result.iterator();
      }
      while (circles.hasNext()) {
        ExtendedCircle circle = circles.next();
        U point = (U) circle.getCenterGeometry();
        final List knnData =
            Arrays.asList(
                ((ExtendedSTRtree) treeIndex)
                    .nearestNeighbourWithDistance(
                        point.getEnvelopeInternal(),
                        point,
                        geometryItemDistance,
                        k,
                        circle.getRadius()));
        result.add(Pair.of(circle.getPartitionId(), new Tuple2<>(point, knnData)));
      }
    }
    return result.iterator();
  }
}
