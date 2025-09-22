package ru.practicum.common.utils;

import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UtilityClass
public class JsonFileManager {

    public static List<Path> getJsonFiles(String dirPath) throws IOException{
        return getJsonFiles(dirPath, false);
    }

    private static List<Path> getJsonFiles(String dirPath, boolean recursive) throws IOException {
        Path dir = Paths.get(dirPath);

        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            throw new IOException("Директория не существует или не является папкой: " + dirPath);
        }

        try (Stream<Path> stream = recursive ? Files.walk(dir) : Files.list(dir)){
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".json"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .collect(Collectors.toList());
        }
    }

    private static List<Path> getJsonFiles(String dirPath, String filenamePattern) throws IOException {
        Path dir = Paths.get(dirPath);

        return Files.list(dir)
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".json"))
                .filter(path -> path.getFileName().toString().matches(filenamePattern))
                .sorted()
                .collect(Collectors.toList());
    }

    private static void removeFile(String filePath) throws IOException {
        Path file = Paths.get(filePath);

        if (!Files.exists(file) || Files.isDirectory(file)){
            throw new IOException("Файл не существует или является папкой: " + filePath);
        }
        
        Files.deleteIfExists(file);
    }

    private static void removeDirectory(String dirPath) throws IOException {
        Path dir = Paths.get(dirPath);

        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            throw new IOException("Директория не существует или не является папкой: " + dirPath);
        }

        Files.deleteIfExists(dir);
    }
}
