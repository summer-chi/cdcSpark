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
package org.locationtech.jts.index.strtree;

import java.util.ArrayList;
import java.util.PriorityQueue;
import org.apache.sedona.core.geometryEntities.GeometryWithDistance;
import org.locationtech.jts.geom.Envelope;

/** @ClassName:ExtendedSTRtree @Description:TODO @Author:yolo @Date:2024/10/912:05 @Version:1.0 */
public class ExtendedSTRtree extends STRtree {
  public ExtendedSTRtree() {
    this(10);
  }

  public ExtendedSTRtree(int nodeCapacity) {
    super(nodeCapacity);
  }

  public ExtendedSTRtree(int nodeCapacity, STRtreeNode root) {
    super(nodeCapacity, root);
  }

  public ExtendedSTRtree(int nodeCapacity, ArrayList itemBoundables) {
    super(nodeCapacity, itemBoundables);
  }

  public GeometryWithDistance[] nearestNeighbourWithDistance(
      Envelope env, Object item, ItemDistance itemDist, int k) {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbourKWithDistance(bp, k);
  }

  public GeometryWithDistance[] nearestNeighbourWithDistance(
      Envelope env, Object item, ItemDistance itemDist, int k, double maxDistance) {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbourKWithDistance(bp, maxDistance, k);
  }

  private GeometryWithDistance[] nearestNeighbourKWithDistance(BoundablePair initBndPair, int k) {
    return nearestNeighbourKWithDistance(initBndPair, Double.POSITIVE_INFINITY, k);
  }

  private GeometryWithDistance[] nearestNeighbourKWithDistance(
      BoundablePair initBndPair, double maxDistance, int k) {
    double distanceLowerBound = maxDistance;

    // initialize internal structures
    PriorityQueue priQ = new PriorityQueue();

    // initialize queue
    priQ.add(initBndPair);

    PriorityQueue kNearestNeighbors = new PriorityQueue();

    while (!priQ.isEmpty() && distanceLowerBound >= 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double pairDistance = bndPair.getDistance();

      /**
       * If the distance for the first node in the queue is >= the current maximum distance in the k
       * queue , all other nodes in the queue must also have a greater distance. So the current
       * minDistance must be the true minimum, and we are done.
       */
      if (pairDistance >= distanceLowerBound) {
        break;
      }
      /**
       * If the pair members are leaves then their distance is the exact lower bound. Update the
       * distanceLowerBound to reflect this (which must be smaller, due to the test immediately
       * prior to this).
       */
      if (bndPair.isLeaves()) {
        // assert: currentDistance < minimumDistanceFound

        if (kNearestNeighbors.size() < k) {
          kNearestNeighbors.add(bndPair);
        } else {

          BoundablePair bp1 = (BoundablePair) kNearestNeighbors.peek();
          if (bp1.getDistance() > pairDistance) {
            kNearestNeighbors.poll();
            kNearestNeighbors.add(bndPair);
          }
          /*
           * minDistance should be the farthest point in the K nearest neighbor queue.
           */
          BoundablePair bp2 = (BoundablePair) kNearestNeighbors.peek();
          distanceLowerBound = bp2.getDistance();
        }
      } else {
        /**
         * Otherwise, expand one side of the pair, (the choice of which side to expand is
         * heuristically determined) and insert the new expanded pairs into the queue
         */
        //                System.out.println("composite: "+bndPair.getBoundable(0).getBounds());
        bndPair.expandToQueue(priQ, distanceLowerBound);
      }
    }
    // done - return items with min distance

    return getItemsWithDistance(kNearestNeighbors);
  }

  private static GeometryWithDistance[] getItemsWithDistance(PriorityQueue kNearestNeighbors) {
    /**
     * Iterate the K Nearest Neighbour Queue and retrieve the item from each BoundablePair in this
     * queue
     */
    GeometryWithDistance[] items = new GeometryWithDistance[kNearestNeighbors.size()];
    int count = 0;
    while (!kNearestNeighbors.isEmpty()) {
      BoundablePair bp = (BoundablePair) kNearestNeighbors.poll();
      items[count] =
          new GeometryWithDistance(
              ((ItemBoundable) bp.getBoundable(0)).getItem(), bp.getDistance());
      count++;
    }
    return items;
  }
}
