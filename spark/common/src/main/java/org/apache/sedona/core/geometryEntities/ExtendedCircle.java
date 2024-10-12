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

import org.apache.sedona.common.geometryObjects.Circle;
import org.locationtech.jts.geom.Geometry;

/** @ClassName:ExtendedCircle @Description:TODO @Author:yolo @Date:2024/10/914:17 @Version:1.0 */
public class ExtendedCircle extends Circle {

  private int partitionId;

  public ExtendedCircle(Geometry centerGeometry, Double givenRadius) {
    super(centerGeometry, givenRadius);
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }
}
