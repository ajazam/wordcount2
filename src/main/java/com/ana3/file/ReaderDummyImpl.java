package com.ana3.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReaderDummyImpl implements Reader{

    private List<String> list = new ArrayList<>();
    private int currentLine = -1;

    ReaderDummyImpl(List<String> list) {
        this.list = list;
    }

    /**
     * Intialise the class with the contents of a file
     * @param filePath Location of xml file e.g src/test/resources/dummy.xml
     */
    ReaderDummyImpl(String filePath){
        File file = new File(filePath);
        String absolutePath = file.getAbsolutePath();
        try (Stream<String> stream = Files.lines(Paths.get(absolutePath))) {
            list = stream.collect(Collectors.toList());
        } catch (IOException ignore) {

        }

    }

    public ReaderDummyImpl(){
        this("src/test/resources/dummy.xml");
    }

    @Override
    public void init() {
    }

    @Override
    public String getLine() {
        currentLine+=1;
        if (currentLine>=list.size()) {
            currentLine--;
            return null;
        }
        return list.get(currentLine);
    }

    @Override
    public void close() {
    }
}