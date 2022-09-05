package com.github.nejckorasa.s3.utils;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@UtilityClass
public class FileUtils {

    @SneakyThrows
    public static String readFileAsString(String path) {
        return Files.readString(Path.of(requireNonNull(currentThread().getContextClassLoader().getResource(path)).toURI()));
    }

    @SneakyThrows
    public static byte[] generateZippedCsv(Path path, String filename, int rows) {
        File dir = path.toFile();
        dir.mkdirs();

        File zipFile = new File(dir, "zipped.zip");
        zipFile.createNewFile();

        var os = new ByteArrayOutputStream();
        try (var zos = new ZipOutputStream(new FileOutputStream(zipFile))) {
            var zipEntry = new ZipEntry(filename);
            zos.putNextEntry(zipEntry);
            byte[] bytes = "COL1, COL2, COL3, COL4\n".getBytes(UTF_8);
            zos.write(bytes);
            os.write(bytes);
            for (int i = 1; i <= rows; i++) {
                bytes = ("val" + i + "_1, val" + i + "_2, val" + i + "_3, val" + i + "_4\n").getBytes(UTF_8);
                zos.write(bytes);
                os.write(bytes);
            }
        }

        return os.toByteArray();
    }
}
