package cs.utils;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ShapeUtils {

    public static Map<String, Set<Statement>> groupStatementsByShape(Model model) {
        Map<String, Set<Statement>> shapeMap = new HashMap<>();
        for (Statement stmt : model) {
            String shape = stmt.getSubject().stringValue();
            shapeMap.computeIfAbsent(shape, k -> new HashSet<>()).add(stmt);
        }
        return shapeMap;
    }

    public static Set<String> analyzeShapes(Map<String, Set<Statement>> oldShapeMap, Map<String, Set<Statement>> newShapeMap) {
        Set<String> affectedShapes = new HashSet<>();

        for (String shape : newShapeMap.keySet()) {
            if (oldShapeMap.containsKey(shape)) {
                if (!Models.isomorphic(oldShapeMap.get(shape), newShapeMap.get(shape))) {
                    affectedShapes.add(shape); // Modification
                }
            } else {
                affectedShapes.add(shape); // Addition
            }
        }

        for (String shape : oldShapeMap.keySet()) {
            if (!newShapeMap.containsKey(shape)) {
                affectedShapes.add(shape); // Deletion
            }
        }

        return affectedShapes;
    }

    public static Map<String, Set<Statement>> checkInconsistencies(Map<String, Set<Statement>> oldShapeMap, Map<String, Set<Statement>> newShapeMap, String inconsistencyCheckFilename, Set<String> affectedShapes) {
        Map<String, Set<Statement>> conflicts = new HashMap<>();

        for (String shape : affectedShapes) {
            if (oldShapeMap.containsKey(shape) && newShapeMap.containsKey(shape)) {
                Set<Statement> oldStatements = oldShapeMap.get(shape);
                Set<Statement> newStatements = newShapeMap.get(shape);

                if (!Models.isomorphic(oldStatements, newStatements)) {
                    conflicts.put(shape, newStatements); // Conflict
                }
            } else if (newShapeMap.containsKey(shape)) {
                conflicts.put(shape, newShapeMap.get(shape)); // Added shape
            } else if (oldShapeMap.containsKey(shape)) {
                conflicts.put(shape, oldShapeMap.get(shape)); // Deleted shape
            }
        }

        return conflicts;
    }

    public static void writeAffectedShapesToFile(Set<String> affectedShapes, Model newShapes, String outputFilename) {
        try (FileWriter writer = new FileWriter(outputFilename)) {
            writer.write("Affected SHACL Shapes:\n");
            for (String shape : affectedShapes) {
                writer.write("Affected Shape: " + shape + "\n");
            }

            Model affectedShapesModel = new LinkedHashModel();
            for (Statement stmt : newShapes) {
                if (affectedShapes.contains(stmt.getSubject().stringValue())) {
                    affectedShapesModel.add(stmt);
                }
            }

            String affectedShapesFile = outputFilename.replace(".txt", "_affected_shapes.ttl");
            try (FileWriter rdfWriter = new FileWriter(affectedShapesFile)) {
                Rio.write(affectedShapesModel, rdfWriter, RDFFormat.TURTLE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeInconsistenciesToFile(Map<String, Set<Statement>> oldShapeMap, Map<String, Set<Statement>> newShapeMap, String outputFilename) {
        try (FileWriter writer = new FileWriter(outputFilename)) {
            writer.write("Inconsistency Check Report:\n");

            for (String shape : oldShapeMap.keySet()) {
                if (newShapeMap.containsKey(shape)) {
                    Set<Statement> oldStatements = oldShapeMap.get(shape);
                    Set<Statement> newStatements = newShapeMap.get(shape);

                    if (!Models.isomorphic(oldStatements, newStatements)) {
                        writer.write("Conflicting Shape: " + shape + "\n");
                        for (Statement stmt : oldStatements) {
                            if (!newStatements.contains(stmt)) {
                                writer.write("Deleted or Modified: " + stmt + "\n");
                            }
                        }
                        for (Statement stmt : newStatements) {
                            if (!oldStatements.contains(stmt)) {
                                writer.write("Added or Modified: " + stmt + "\n");
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Model readModel(String filePath) {
        Model model = new LinkedHashModel();
        try (FileInputStream inputStream = new FileInputStream(filePath)) {
            RDFFormat format = Rio.getParserFormatForFileName(filePath).orElse(RDFFormat.TURTLE);
            RDFParser rdfParser = Rio.createParser(format);
            rdfParser.setRDFHandler(new StatementCollector(model));
            rdfParser.parse(inputStream, "");
        } catch (IOException | RDFParseException | RDFHandlerException e) {
            e.printStackTrace();
        }
        return model;
    }
}
