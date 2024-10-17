package cs.cose;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class StatisticalAnalysis {

    public static void performStatisticalAnalysis(String outputDir, String datasetName) {
        Map<Integer, Integer> changeCounts = new HashMap<>();
        Map<Integer, Integer> shapeCounts = new HashMap<>();
        Map<Integer, Long> times = new HashMap<>();

        for (int depth = 0; depth <= 10; depth++) {
            String subgraphDir = outputDir + "Subgraphs/" + datasetName + "/";
            String subgraphFile = subgraphDir + "subgraph_depth_" + depth + ".nt";
            changeCounts.put(depth, countChanges(subgraphFile));
            shapeCounts.put(depth, countShapes(outputDir + "SHACL-Shapes/" + datasetName + "/SHACL_shapes_depth_" + depth + ".ttl"));
            times.put(depth, getTime(outputDir + "Subgraphs/" + datasetName + "/" + "time_depth_" + depth + ".txt"));
        }

        saveStatistics(outputDir + "OptimalDepth/" + datasetName + "/", changeCounts, shapeCounts, times);

        int optimalDepth = findOptimalDepth(changeCounts, shapeCounts, times);
        saveOptimalDepth(outputDir + "OptimalDepth/" + datasetName + "/", optimalDepth);
    }

    private static int countChanges(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            return (int) reader.lines().count();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static int countShapes(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            return (int) reader.lines().filter(line -> line.trim().length() > 0).count();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static long getTime(String filePath) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            if (!lines.isEmpty()) {
                return Long.parseLong(lines.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static int findOptimalDepth(Map<Integer, Integer> changeCounts, Map<Integer, Integer> shapeCounts, Map<Integer, Long> times) {
        int optimalDepth = 0;
        int minChanges = Integer.MAX_VALUE;

        for (Map.Entry<Integer, Integer> entry : changeCounts.entrySet()) {
            int depth = entry.getKey();
            int changes = entry.getValue();

            if (changes < minChanges) {
                minChanges = changes;
                optimalDepth = depth;
            }
        }

        return optimalDepth;
    }

    private static void saveOptimalDepth(String outputDir, int optimalDepth) {
        new File(outputDir).mkdirs();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "optimal_depth.txt"))) {
            writer.write("Optimal Depth: " + optimalDepth);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveStatistics(String outputDir, Map<Integer, Integer> changeCounts, Map<Integer, Integer> shapeCounts, Map<Integer, Long> times) {
        new File(outputDir).mkdirs();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "statistics.txt"))) {
            writer.write("Depth\tChanges\tShapes\tTime\n");
            for (int depth = 0; depth <= 10; depth++) {
                writer.write(depth + "\t" + changeCounts.get(depth) + "\t" + shapeCounts.get(depth) + "\t" + times.get(depth) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
