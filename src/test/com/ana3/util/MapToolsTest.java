package com.ana3.util;

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MapToolsTest {

    @Test
    public void contact2Test1() {
        Map<String, Long> map1 = new HashMap<>();
        map1.put("red", 1L);
        map1.put("green", 2L);
        map1.put("blue", 3L);

        Map<String, Long> map2 = new HashMap<>();
        map2.put("red", 1L);
        map2.put("green", 2L);
        map2.put("blue", 3L);

        Map<String, Long> results = MapTools.concat2(map1, map2);

        assertEquals(new Long(2), results.get("red"));
        assertEquals(new Long(4), results.get("green"));
        assertEquals(new Long(6), results.get("blue"));
    }

    @Test
    public void testReverseSortCount() {
        Map<String, Long> words = new HashMap<>();
        words.put("abacus", 5L);
        words.put("banana", 1L);
        words.put("zebra", 6L);

        System.out.printf("words are " + words);

        Map<String, Long> sortedWords = MapTools.reverseCountSort(words);
        assertEquals(sortedWords, words);
    }

    @Test
    public void testMaxWordCount() {
        Map<String, Long> words = new HashMap<>();
        words.put("abacus", 5L);
        words.put("banana", 1L);
        words.put("zebra", 6L);
        Map<String, Long> sortedWords = MapTools.reverseCountSort(words);

        Map.Entry<String, Long> entry = MapTools.wordWithHighCount(sortedWords);

        assertEquals("zebra", entry.getKey());
        assertEquals(new Long(6), entry.getValue());
    }

    @Test
    public void testMinWordCount() {
        Map<String, Long> words = new HashMap<>();
        words.put("abacus", 5L);
        words.put("banana", 1L);
        words.put("zebra", 6L);
        Map<String, Long> sortedWords = MapTools.reverseCountSort(words);

        Map.Entry<String, Long> entry = MapTools.wordWithLowCount(sortedWords);

        assertEquals("banana", entry.getKey());
        assertEquals(new Long(1), entry.getValue());
    }
}
