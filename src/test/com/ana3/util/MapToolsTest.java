package com.ana3.util;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

public class MapToolsTest {
//    @Test
//    public void concatTest(){
//        Map<String, Long> map1 = new HashMap<>();
//        map1.put("red", 1L);
//        map1.put("green", 2L);
//        map1.put("blue", 3L);
//
//        Map<String, Long> map2 = new HashMap<>();
//        map2.put("red", 1L);
//        map2.put("green", 2L);
//        map2.put("blue", 3L);
//
//        Map<String, Long> results = MapTools.concat(map1, map2);
//
//        assertEquals(new Long(2), results.get("red"));
//        assertEquals(new Long(4), results.get("green"));
//        assertEquals(new Long(6), results.get("blue"));
//    }

    @Test
    public void contact2Test1(){
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
}
