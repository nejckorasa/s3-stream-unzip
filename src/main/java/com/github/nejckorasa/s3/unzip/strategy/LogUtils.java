package com.github.nejckorasa.s3.unzip.strategy;

import lombok.experimental.UtilityClass;

import java.text.StringCharacterIterator;

@UtilityClass
class LogUtils {

    static String humanReadableBytes(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        var ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }
}
