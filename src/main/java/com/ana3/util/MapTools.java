package com.ana3.util;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapTools {

    public static Map<String, Long> concatna(Map<String, Long> map1, Map<String, Long> map2) {
        return Stream.of(map1, map2).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Math::addExact));
    }

    public static Map<String, Long> concat2(Map<String, Long> map1, Map<String, Long> map2) {
        Set<String> keys = new LinkedHashSet<>();
        keys.addAll(map1.keySet());
        keys.addAll(map2.keySet());

        Map<String, Long> newMap = new HashMap<String, Long>();

        for (String key : keys) {
            newMap.put(key, map1.getOrDefault(key, 0L) + map2.getOrDefault(key, 0L));
        }

        return newMap;


    }
}
