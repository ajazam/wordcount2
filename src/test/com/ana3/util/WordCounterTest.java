package com.ana3.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.*;

public class WordCounterTest {
    @Test
    public void wordCountTest(){
        List<String> lines = new ArrayList<>();
        lines.add("the cat sat of the mat");
        lines.add("the cat on a hat");

        Map<String, Long> count = WordCounter.count(lines);

        Map<String, Long> expectedNamesAndValues = new HashMap<>();
        expectedNamesAndValues.put("the", 3L);
        expectedNamesAndValues.put("a", 1L);
        expectedNamesAndValues.put("mat", 1L);
        expectedNamesAndValues.put("of", 1L);
        expectedNamesAndValues.put("sat", 1L);
        expectedNamesAndValues.put("cat", 2L);
        expectedNamesAndValues.put("hat", 1L);
        expectedNamesAndValues.put("on", 1L);
        assertEquals(expectedNamesAndValues, count);
        System.out.printf("wordcounts are "+expectedNamesAndValues);
    }
}
