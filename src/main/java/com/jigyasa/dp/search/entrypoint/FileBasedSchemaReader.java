package com.jigyasa.dp.search.entrypoint;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.utils.SchemaUtil;
import lombok.RequiredArgsConstructor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@RequiredArgsConstructor
public class FileBasedSchemaReader implements IndexSchemaReader {

    final private Optional<Path> filePath;

    @Override
    public IndexSchema readSchema() {
        try {
            if (filePath.isEmpty()) {
                System.out.println("Alert....!! Using dummy schema as server is started in test mode!");
                return SchemaUtil.parseSchema(Files.readString(Path.of(this.getClass().getResource("/schema/SampleSchema.json").toURI())));
            }
            return SchemaUtil.parseSchema(Files.readString(filePath.get()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read index schema from file location: " + this.filePath, e);
        }
    }
}
