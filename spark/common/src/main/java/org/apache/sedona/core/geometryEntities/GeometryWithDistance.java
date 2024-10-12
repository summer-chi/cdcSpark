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
import org.locationtech.jts.geom.Geometry;

/** @ClassName:PointWithDistance @Description:TODO @Author:yolo @Date:2024/10/99:31 @Version:1.0 */
public class GeometryWithDistance<T extends Geometry> implements Serializable {
  T geometry;
  double distance;
  long indexID;

  public GeometryWithDistance(Object geom, double distance) {
    this.geometry = (T) geom;
    this.distance = distance;
    if (geometry.getUserData() instanceof String) {
      String[] split = ((String) geometry.getUserData()).split(",|\\s+|;");
      this.indexID = Long.parseLong(split[1]);
    } else if (geometry.getUserData() instanceof CDCPoint.UserData) {
      this.indexID = ((CDCPoint.UserData) geometry.getUserData()).getIndexID();
    }
  }

  public GeometryWithDistance(T geom, double distance, long indexID) {
    this.geometry = geom;
    this.distance = distance;
    this.indexID = indexID;
  }

  public T getGeometry() {
    return geometry;
  }

  public double getDistance() {
    return distance;
  }

  public long getIndexID() {
    return indexID;
  }

  @Override
  public String toString() {
    return "PointDistance{" + "distance=" + distance + ", indexID=" + indexID + '}';
  }
}
