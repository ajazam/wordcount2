package com.ana3.file;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;

public class ReaderImplTest {
    @Test
    public void getLineTest(){
        File file = new File("src/test/resources/dummy.xml");
        String absolutePath = file.getAbsolutePath();

        ReaderImpl reader = new ReaderImpl(absolutePath);
        reader.init();
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", reader.getLine());
        assertEquals("", reader.getLine());
        assertEquals("<letter>", reader.getLine());
        for(int i=0; i<35;i++) {
            reader.getLine();
        }
        assertEquals("</letter>", reader.getLine());

        assertNull(reader.getLine());

        reader.close();
    }
}
