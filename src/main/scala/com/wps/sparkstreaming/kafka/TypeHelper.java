package com.wps.sparkstreaming.kafka;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import java.util.ArrayList;
import java.util.List;

public class TypeHelper {
    @SuppressWarnings("unchecked")
    public static <K,V> Map<K,V> toScalaImmutableMap(java.util.Map<K,V> javaMap) {
        final List<Tuple2<K,V>> list = new ArrayList<>(javaMap.size());
        for (java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
            list.add(Tuple2.apply(entry.getKey(),entry.getValue()));
        }
        final Seq<Tuple2<K, V>> seq = JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
        return (Map<K,V>) Map$.MODULE$.apply(seq);
    }
}
