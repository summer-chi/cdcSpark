package org.apache.sedona.core.geometryEntities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.apache.sedona.common.geometrySerde.GeometrySerde;
import org.locationtech.jts.geom.Point;

/**
 * @ClassName:PointWithDistanceSerde
 * @Description:TODO
 * @Author:yolo
 * @Date:2024/10/910:08
 * @Version:1.0
 */
public class PointWithDistanceSerde extends GeometrySerde {
    private static final Logger log = Logger.getLogger(GeometrySerde.class);

    @Override
    public void write(Kryo kryo, Output out, Object object) {
        PointWithDistance geom=(PointWithDistance) object;
        super.write(kryo, out, geom.point);
        out.writeDouble(geom.distance);
        out.writeLong(geom.indexID);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        Point point = (Point) super.read(kryo, input, aClass);
        double distance = input.readDouble();
        long indexId = input.readLong();
        return new PointWithDistance(point, distance, indexId);
    }
}
