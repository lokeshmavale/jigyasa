package com.jigyasa.dp.search.entrypoint;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.utils.SchemaUtil;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@RequiredArgsConstructor
public class FileBasedSchemaReader implements IndexSchemaReader {
    private static final Logger log = LoggerFactory.getLogger(FileBasedSchemaReader.class);

    final private Optional<Path> filePath;

    @Override
    public IndexSchema readSchema() {
        try {
            if (filePath.isEmpty()) {
                log.warn("No schema path configured — using built-in SampleSchema.json (test mode)");
                try (InputStream is = getClass().getResourceAsStream("/schema/SampleSchema.json")) {
                    if (is == null) {
                        throw new IllegalStateException("Built-in SampleSchema.json not found on classpath");
                    }
                    return SchemaUtil.parseSchema(new String(is.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
            log.info("Loading schema from {}", filePath.get());
            return SchemaUtil.parseSchema(Files.readString(filePath.get()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read index schema from file location: " + this.filePath, e);
        }
    }
}
