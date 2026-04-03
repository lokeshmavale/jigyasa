package com.jigyasa.dp.search.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class FileUtils {

    private FileUtils() {}

    public static List<Path> listFilesWithPrefix(Path dir, String prefix, boolean sort) {
        List<Path> paths = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, path -> path.getFileName().toString().startsWith(prefix))) {
            for (Path path : stream) {
                paths.add(path);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to list translog files", e);
        }

        if (sort) {
            paths.sort((o1, o2) -> {
                int o1Num = Integer.parseInt(StringUtils.substringAfterLast(o1.getFileName().toString(), "."));
                int o2Num = Integer.parseInt(StringUtils.substringAfterLast(o2.getFileName().toString(), "."));
                return Integer.compare(o1Num, o2Num);
            });
        }

        return paths;
    }
}
