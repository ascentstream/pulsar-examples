package com.ascentstream.demo.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileUtil {
    public static void writeFileWithAppend(String filePath, String content) {
        FileWriter writer;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new FileWriter(file, true);
            writer.write(content);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void writeFileWithCover(String filePath, String content) {
        FileWriter writer;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new FileWriter(file);
            writer.write(content);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
