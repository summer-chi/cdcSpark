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
package org.apache.sedona.core.geometryEntities;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.locationtech.jts.geom.*;

/** @ClassName:CDCPoint @Description:TODO @Author:yolo @Date:2024/10/99:29 @Version:1.0 */
public class CDCPoint extends Point {
  public CDCPoint(CoordinateSequence coordinates, GeometryFactory factory) {
    super(coordinates, factory);
  }

  public CDCPoint(Point point, List<GeometryWithDistance> knnList) {
    super(point.getCoordinateSequence(), point.getFactory());
    UserData userData = new UserData(point, knnList);
    setUserData(userData);
  }

  public CDCPoint(Point point) {
    super(point.getCoordinateSequence(), point.getFactory());
    setUserData(point.getUserData());
  }

  public static class UserData implements Serializable {
    private long indexID = -1;
    private int clusterID = -1;
    private int neighborsNum = 0;
    private long[] neighborsIndexID;
    private double[] neighborsDistance;
    private double reachDistance = Double.NEGATIVE_INFINITY;
    private double dcmValue = 0;
    private boolean visited = false;
    private Flag flag = Flag.NotFlagged;

    public enum Flag {
      Border,
      Inner,
      NotFlagged
    }

    public UserData(Point point, List<GeometryWithDistance> knnList) {
      String[] split = ((String) point.getUserData()).split(",|\\s+|;");
      this.indexID = Long.parseLong(split[1]);
      this.neighborsNum = knnList.size();
      this.neighborsIndexID = new long[neighborsNum];
      this.neighborsDistance = new double[neighborsNum];
      calculateDCM(point, knnList);
    }

    public UserData(long indexID) {
      this.indexID = indexID;
    }

    public void calculateDCM(Point point, List<GeometryWithDistance> knnList) {
      double[] angleArray = new double[neighborsNum];
      for (int i = 0; i < neighborsNum; i++) {
        Point neighbor = (Point) knnList.get(i).getGeometry();
        double deltaX = neighbor.getX() - point.getX();
        double deltaY = neighbor.getY() - point.getY();
        if (deltaX == 0) {
          if (deltaY == 0) {
            angleArray[i] = 0;
          } else if (deltaY > 0) {
            angleArray[i] = Math.PI / 2;
          } else {
            angleArray[i] = 3 * Math.PI / 2;
          }
        } else if (deltaX > 0) {
          if (Math.atan(deltaY / deltaX) >= 0) {
            angleArray[i] = Math.atan(deltaY / deltaX);
          } else {
            angleArray[i] = 2 * Math.PI + Math.atan(deltaY / deltaX);
          }
        } else {
          angleArray[i] = Math.PI + Math.atan(deltaY / deltaX);
        }
        //            this.neighborsIndexID[neighborsNum - i - 1] = knnList.get(i).getIndexID();
        this.neighborsIndexID[i] = knnList.get(i).getIndexID();
        //            this.neighborsDistance[neighborsNum - i - 1] = knnList.get(i).getDistance();
        this.neighborsDistance[i] = knnList.get(i).getDistance();
      }
      //            System.out.println(Arrays.toString(this.neighborsDistance));

      double[] angleSorted = Arrays.stream(angleArray).sorted().toArray();
      double dcmValue = 0;
      for (int i = 1; i < neighborsNum - 1; i++) {
        dcmValue +=
            Math.pow(angleSorted[i + 1] - angleSorted[i] - 2 * Math.PI / (neighborsNum - 1), 2);
      }
      dcmValue +=
          Math.pow(
              angleSorted[1]
                  - angleSorted[neighborsNum - 1]
                  + 2 * Math.PI
                  - 2 * Math.PI / (neighborsNum - 1),
              2);
      dcmValue /= ((neighborsNum - 2) * 4 * Math.pow(Math.PI, 2) / (neighborsNum - 1));
      this.dcmValue = dcmValue;
    }

    public long getIndexID() {
      return indexID;
    }

    public void setIndexID(long indexID) {
      this.indexID = indexID;
    }

    public int getClusterID() {
      return clusterID;
    }

    public void setClusterID(int clusterID) {
      this.clusterID = clusterID;
    }

    public int getNeighborsNum() {
      return neighborsNum;
    }

    public void setNeighborsNum(int neighborsNum) {
      this.neighborsNum = neighborsNum;
    }

    public long[] getNeighborsIndexID() {
      return neighborsIndexID;
    }

    public void setNeighborsIndexID(long[] neighborsIndexID) {
      this.neighborsIndexID = neighborsIndexID;
    }

    public double[] getNeighborsDistance() {
      return neighborsDistance;
    }

    public void setNeighborsDistance(double[] neighborsDistance) {
      this.neighborsDistance = neighborsDistance;
    }

    public double getReachDistance() {
      return reachDistance;
    }

    public void setReachDistance(double reachDistance) {
      this.reachDistance = reachDistance;
    }

    public double getDcmValue() {
      return dcmValue;
    }

    public void setDcmValue(double dcmValue) {
      this.dcmValue = dcmValue;
    }

    public boolean isVisited() {
      return visited;
    }

    public void setVisited(boolean visited) {
      this.visited = visited;
    }

    public Flag getFlag() {
      return flag;
    }

    public void setFlag(Flag flag) {
      this.flag = flag;
    }
  }

  public void clearUserData() {
    setUserData(new UserData(((UserData) this.getUserData()).getIndexID()));
  }
}
