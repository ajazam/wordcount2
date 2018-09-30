package com.ana3.util;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class MapTools {

    public static Map<String, Long> concatna(Map<String, Long> map1, Map<String, Long> map2) {
        return Stream.of(map1, map2).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Math::addExact));
    }

    public static Map<String, Long> concat2(Map<String, Long> map1, Map<String, Long> map2) {
        Set<String> keys = new LinkedHashSet<>();
        keys.addAll(map1.keySet());
        keys.addAll(map2.keySet());

        Map<String, Long> newMap = new HashMap<>();

        for (String key : keys) {
            newMap.put(key, map1.getOrDefault(key, 0L) + map2.getOrDefault(key, 0L));
        }

        return newMap;
    }

    public static LinkedHashMap<String, Long> reverseCountSort(Map<String, Long> wordsAndCounts) {
        return wordsAndCounts.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
    }

    public static Map.Entry<String, Long> wordWithHighCount(Map<String, Long> words) {

        Iterator<Map.Entry<String, Long>> entryInterator = words.entrySet().iterator();
        Map.Entry<String, Long> entry = null;

        while (entryInterator.hasNext()) {
            Map.Entry<String, Long> currentEntry = entryInterator.next();
            if (entry == null) entry = currentEntry;
        }

        return entry;
    }

    public static Map.Entry<String, Long> wordWithLowCount(Map<String, Long> words) {

        Iterator<Map.Entry<String, Long>> entryInterator = words.entrySet().iterator();
        Map.Entry<String, Long> entry = null;

        while (entryInterator.hasNext()) {
            entry = entryInterator.next();
        }

        return entry;
    }

}
