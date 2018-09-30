package com.ana3.file;

import java.io.*;

public class ReaderImpl implements Reader {
    private String filepath;
    private BufferedReader br;

    public ReaderImpl(String filePath) {
        this.filepath = filePath;
    }

    public void init() {
        try {
            br = new BufferedReader(new FileReader(new File(filepath)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getLine() {
        if (br == null) return null;
        String line = null;
        try {
            line = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }

    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
