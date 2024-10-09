package org.apache.sedona.core.geometryEntities;

import org.locationtech.jts.geom.Point;

import java.io.Serializable;

/**
 * @ClassName:PointWithDistance
 * @Description:TODO
 * @Author:yolo
 * @Date:2024/10/99:31
 * @Version:1.0
 */
public class PointWithDistance implements Serializable {
    Point point;
    double distance;
    long indexID;

    public PointWithDistance(Object point, double distance) {
        this.point = (Point) point;
        this.distance = distance;
        if (((Point) point).getUserData() instanceof String) {
            String[] split = ((String) ((Point) point).getUserData()).split(",|\\s+|;");
            this.indexID = Long.parseLong(split[split.length - 1]);
        } else if (((CDCPoint) point).getUserData() instanceof CDCPoint.UserData) {
            this.indexID = ((CDCPoint.UserData) ((CDCPoint) point).getUserData()).getIndexID();
        }
    }

    public PointWithDistance(Point point, double distance, long indexID) {
        this.point = point;
        this.distance = distance;
        this.indexID = indexID;
    }

    public Point getPoint() {
        return point;
    }

    public double getDistance() {
        return distance;
    }

    public long getIndexID() {
        return indexID;
    }

    @Override
    public String toString() {
        return "PointDistance{" +
                "distance=" + distance +
                ", indexID=" + indexID +
                '}';
    }
}
