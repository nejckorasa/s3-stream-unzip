package io.github.nejckorasa.s3.upload;

import lombok.experimental.UtilityClass;

import java.util.function.Supplier;

@UtilityClass
class Assertions {

    static void assertNotBlank(String str, String errorMessage) {
        assertOrThrow(() -> str == null || str.isBlank(), errorMessage);
    }

    static void assertOrThrow(Supplier<Boolean> assertion, String errorMessage) {
        if (assertion.get()) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
