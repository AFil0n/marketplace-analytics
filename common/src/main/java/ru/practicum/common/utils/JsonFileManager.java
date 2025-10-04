package ru.practicum.common.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.experimental.UtilityClass;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UtilityClass
public class JsonFileManager {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int CHUNK_SIZE = 1000;
    private final String successFolder = "success";
    private final String progressFolder = "in_progress";
    private final String chunksFolder = "chunks";

    public static List<Path> getJsonFiles(String dirPath) throws IOException {
        return getJsonFiles(dirPath, false);
    }

    public static void splitIntoChunks(String filePath) throws IOException {
        Path file = Paths.get(filePath);
        isFile(file, filePath);

        Files.createDirectories(Paths.get(progressFolder + "/" + file.getFileName()));

        List<Path> chunks = splitJsonArray(filePath, "/" + chunksFolder + "/", CHUNK_SIZE);
        Files.delete(file);
        Files.move(chunks.get(0).getParent(), Paths.get(progressFolder));
    }

    public static List<Path> splitJsonArray(String file, String outputDir, int chunkSize) throws IOException {
        Path outputPath = Paths.get(outputDir);
        if (!Files.exists(outputPath)) {
            Files.createDirectories(outputPath);
        }

        List<Path> chunks = new ArrayList<>();
        int fileCounter = 1;
        int recordCounter = 0;

        JsonFactory factory = new JsonFactory();

        try (JsonParser parser = factory.createParser(new File(file))) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IOException("Expected JSON array");
            }

            BufferedWriter currentWriter = null;

            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (recordCounter % chunkSize == 0) {
                    if (currentWriter == null) {
                        currentWriter.write("\n]");
                        currentWriter.close();
                    }

                    Path chunk = outputPath.resolve(String.format("%d.json", fileCounter));
                    currentWriter = new BufferedWriter(new FileWriter(chunk.toFile()));
                    currentWriter.write("[\n");
                    chunks.add(chunk);
                    fileCounter++;
                    recordCounter = 0;
                }

                ObjectNode node = objectMapper.readTree(parser);
                String json = objectMapper.writeValueAsString(node);

                if (recordCounter > 0) {
                    currentWriter.write(",\n");
                }
                currentWriter.write(json);

                recordCounter++;

                if (parser.nextToken() == JsonToken.END_ARRAY) {
                    break;
                }
            }

            if (currentWriter != null) {
                currentWriter.write("\n]");
                currentWriter.close();
            }

            System.out.printf("Разбито на %d файлов по %d записей каждый\n",
                    chunks.size(), chunkSize);
            return chunks;
        }
    }

    private static List<Path> getJsonFiles(String dirPath, boolean recursive) throws IOException {
        Path dir = Paths.get(dirPath);
        isDirectory(dir, dirPath);

        try (Stream<Path> stream = recursive ? Files.walk(dir) : Files.list(dir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".json"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .collect(Collectors.toList());
        }
    }

    public static Path getFirstJsonFile(String dirPath) throws IOException {
        return Files.list(Paths.get(dirPath))
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".json"))
                .sorted(Comparator.comparing(Path::getFileName))
                .findFirst().get();
    }

    public static Path getAnyJsonFile(String dirPath) throws IOException {
        return Files.list(Paths.get(dirPath))
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".json"))
                .sorted(Comparator.comparing(Path::getFileName))
                .findAny().get();
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

    public static void removeFile(String filePath) throws IOException {
        Path file = Paths.get(filePath);
        isFile(file, filePath);
        Files.deleteIfExists(file);
    }

    public static void removeDirectory(String dirPath) throws IOException {
        Path dir = Paths.get(dirPath);
        isDirectory(dir, dirPath);

        Files.deleteIfExists(dir);
    }

    private static void isFile(Path file, String filePath) throws IOException {
        if (!Files.exists(file) || Files.isDirectory(file)) {
            throw new IOException("Файл не существует или является папкой: " + filePath);
        }
    }

    private static void isDirectory(Path file, String filePath) throws IOException {
        if (!Files.exists(file) || Files.isDirectory(file)) {
            throw new IOException("Файл не существует или является папкой: " + filePath);
        }
    }

    public static void moveFile(Path sourcePath, String targetDir) throws IOException {
        Path targetDirPath = Paths.get(targetDir);

        // Создаем целевую директорию, если она не существует
        if (!Files.exists(targetDirPath)) {
            Files.createDirectories(targetDirPath);
        }

        Path targetPath = targetDirPath.resolve(sourcePath.getFileName());
        Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
    }
}
