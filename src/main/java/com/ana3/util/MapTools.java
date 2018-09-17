package com.ana3.util;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapTools {

    public static Map<String, Long> concat(Map<String, Long> map1, Map<String, Long> map2){
        return Stream.of(map1, map2).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Math::addExact));
    }
}
