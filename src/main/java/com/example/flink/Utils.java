package com.example.flink;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {
    /**
     * Converts a relative path (relative to the current working directory)
     * to an absolute path and then normalizes it.
     *
     * @param relativePathString The relative path string.
     * @return An absolute and normalized Path object.
     */
    public static String convertAndNormalizePath(String relativePathString) {
        // Create a Path object from the relative path string.
        // Paths.get() handles OS-specific path separators automatically.
        Path relativePath = Paths.get(relativePathString);

        // Convert the relative path to an absolute path based on the current working
        // directory.
        // E.g., if working dir is /Users/user/myproject and relativePath is
        // "data/file.txt",
        // toAbsolutePath() will result in /Users/user/myproject/data/file.txt.
        Path absolutePath = relativePath.toAbsolutePath();

        // Normalize the path by removing redundant elements like "." and ".."
        // E.g., /a/./b/../c will be normalized to /a/c.
        // This operation is purely syntactic and does not access the file system.
        Path normalizedPath = absolutePath.normalize();

        return normalizedPath.toString();
    }

    /**
     * Converts a file path to a URI format.
     * 
     * @param filePath The file path to be converted.
     * @return A string representing the file URI.
     */
    public static String getFileURI(String filePath) {
        filePath = convertAndNormalizePath(filePath);
        return "file://" + filePath.replace("\\", "/");
    }
}
