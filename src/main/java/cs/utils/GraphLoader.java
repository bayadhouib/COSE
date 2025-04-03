package cs.utils;

import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.vocabulary.RDF;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GraphLoader {

    public static Model loadJenaModel(String filePath, Lang lang) throws IOException {
        validateFilePath(filePath);

        Model model = ModelFactory.createDefaultModel();
        try (InputStream input = new FileInputStream(filePath)) {
            RDFParser.source(input).lang(lang).parse(model.getGraph());
        } catch (Exception e) {
            System.err.println("Error loading Jena model from " + filePath + ": " + e.getMessage());
            throw new IOException("Failed to load Jena model. File: " + filePath, e);
        }
        return model;
    }


    public static void saveJenaModel(Model model, Lang lang, String filePath) throws IOException {
        validateFilePathForWriting(filePath);

        try (OutputStream output = new FileOutputStream(filePath)) {
            RDFDataMgr.write(output, model, lang);
            System.out.println("Jena model successfully saved to " + filePath);
        } catch (Exception e) {
            System.err.println("Error saving Jena model to " + filePath + ": " + e.getMessage());
            throw new IOException("Failed to save Jena model. File: " + filePath, e);
        }
    }


    public static Map<Resource, Map<Property, Set<RDFNode>>> loadGraphAsMap(String filePath, Lang lang) throws IOException {
        validateFilePath(filePath);

        Map<Resource, Map<Property, Set<RDFNode>>> graphMap = new ConcurrentHashMap<>();
        Model jenaModel = loadJenaModel(filePath, lang);

        jenaModel.listStatements().forEachRemaining(statement -> {
            graphMap.computeIfAbsent(statement.getSubject(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(statement.getPredicate(), k -> ConcurrentHashMap.newKeySet())
                    .add(statement.getObject());
        });

        return graphMap;
    }

    public static Map<String, Set<String>> computeClassToPropertyMap(Map<Resource, Map<Property, Set<RDFNode>>> graphMap) {
        if (graphMap == null || graphMap.isEmpty()) {
            throw new IllegalArgumentException("Graph map cannot be null or empty.");
        }

        Map<String, Set<String>> classToPropertyMap = new HashMap<>();

        graphMap.forEach((resource, properties) -> {
            properties.forEach((property, values) -> {
                if (property.equals(RDF.type)) {
                    values.forEach(value -> {
                        String classType = value.toString();
                        classToPropertyMap.computeIfAbsent(classType, k -> new HashSet<>())
                                .add(property.getURI());
                    });
                }
            });
        });

        return classToPropertyMap;
    }


    private static void validateFilePath(String filePath) throws IOException {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty.");
        }

        if (!Files.exists(Paths.get(filePath))) {
            throw new IOException("File does not exist: " + filePath);
        }

        if (!Files.isReadable(Paths.get(filePath))) {
            throw new IOException("File is not readable: " + filePath);
        }
    }


    private static void validateFilePathForWriting(String filePath) throws IOException {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty.");
        }

        Path path = Paths.get(filePath);
        if (Files.exists(path) && !Files.isWritable(path)) {
            throw new IOException("File exists but is not writable: " + filePath);
        }

        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent());
        }
    }
}
