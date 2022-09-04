package com.github.nejckorasa.s3.utils;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.io.BufferedWriter;
import java.io.FileWriter;

@UtilityClass
public class FileUtils {

    @SneakyThrows
    public static void generateCsv(String filename, int rows) {
        try (var writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write("COL1, COL2, COL3, COL4\n");
            for (int i = 1; i <= rows; i++) {
                writer.write("val" + i + "_1, val" + i + "_2, val" + i + "_3, val" + i + "_4\n");
            }
        }
    }
}
