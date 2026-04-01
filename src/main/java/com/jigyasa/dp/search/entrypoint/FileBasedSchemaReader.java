package com.jigyasa.dp.search.entrypoint;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.utils.SchemaUtil;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                return SchemaUtil.parseSchema(Files.readString(Path.of(this.getClass().getResource("/schema/SampleSchema.json").toURI())));
            }
            log.info("Loading schema from {}", filePath.get());
            return SchemaUtil.parseSchema(Files.readString(filePath.get()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read index schema from file location: " + this.filePath, e);
        }
    }
}
