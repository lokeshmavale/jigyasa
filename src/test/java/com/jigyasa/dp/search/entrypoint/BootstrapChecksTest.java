package com.jigyasa.dp.search.entrypoint;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class BootstrapChecksTest {

    @Test
    @DisplayName("run() does not throw when no env vars are set")
    void run_doesNotThrow_whenNoEnvVarsSet() {
        assertThatCode(BootstrapChecks::run).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("run() completes successfully without BOOTSTRAP_MEMORY_LOCK")
    void run_completesSuccessfully() {
        // In test environment BOOTSTRAP_MEMORY_LOCK is not set,
        // so the memory-lock branch is skipped — run() should still complete.
        BootstrapChecks.run();
    }
}
