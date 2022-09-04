package com.github.nejckorasa.s3;

import lombok.experimental.UtilityClass;

import java.util.function.Supplier;

@UtilityClass
public class Assertions {

    public static void assertNotBlank(String str, String errorMessage) {
        assertOrThrow(() -> str == null || str.isBlank(), errorMessage);
    }

    public static void assertOrThrow(Supplier<Boolean> assertion, String errorMessage) {
        if (assertion.get()) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
