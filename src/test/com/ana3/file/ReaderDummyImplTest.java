package com.ana3.file;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class ReaderDummyImplTest {

    @Test
    public void getLineUsingListTest(){
        List<String> lines = new ArrayList<>();
        lines.add("1");
        lines.add("2");

        ReaderDummyImpl reader = new ReaderDummyImpl(lines);
        assertEquals("1", reader.getLine());
        assertEquals("2", reader.getLine());
        assertNull(reader.getLine());
        assertNull(reader.getLine());
    }

    @Test
    public void getLineUsingFileTest(){
        ReaderDummyImpl reader = new ReaderDummyImpl("src/test/resources/dummy.xml");
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", reader.getLine());
        assertEquals("", reader.getLine());
        assertEquals("<letter>", reader.getLine());

        for(int i=0; i<35;i++) {
            reader.getLine();
        }

        assertEquals("</letter>", reader.getLine());
        assertNull(reader.getLine());
    }
}
