package cs.cose;

import cs.Main;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ChangeDetection {

    public static class Change {
        public enum ChangeType { ADDITION, DELETION, MODIFICATION }

        public ChangeType type;
        public String entity;
        public String property;
        public String oldValue;
        public String newValue;

        public Change(ChangeType type, String entity, String property, String oldValue, String newValue) {
            this.type = type;
            this.entity = entity;
            this.property = property;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        @Override
        public String toString() {
            return "Change{" +
                    "type=" + type +
                    ", entity='" + entity + '\'' +
                    ", property='" + property + '\'' +
                    ", oldValue='" + oldValue + '\'' +
                    ", newValue='" + newValue + '\'' +
                    '}';
        }
    }

    /**
     * Detects changes between two versions of a knowledge graph (KG1 and KG2).
     * It tracks additions, deletions, and modifications.
     */
    public List<Change> detectChanges(Model kg1, Model kg2) {
        List<Change> changes = new ArrayList<>();

        // Convert KG1 and KG2 to map structures for efficient comparison
        Map<Resource, Map<IRI, String>> kg1Map = convertToSimpleMap(kg1);
        Map<Resource, Map<IRI, String>> kg2Map = convertToSimpleMap(kg2);

        // Detect changes by comparing kg1 and kg2
        for (Resource entity : kg2Map.keySet()) {
            if (!kg1Map.containsKey(entity)) {
                // If the entity is new in kg2, all its properties are additions
                for (IRI property : kg2Map.get(entity).keySet()) {
                    changes.add(new Change(Change.ChangeType.ADDITION, entity.stringValue(), property.stringValue(), null, kg2Map.get(entity).get(property)));
                }
            } else {
                // Check each property of the existing entity for modifications or additions
                for (IRI property : kg2Map.get(entity).keySet()) {
                    if (!kg1Map.get(entity).containsKey(property)) {
                        // New property added to an existing entity
                        changes.add(new Change(Change.ChangeType.ADDITION, entity.stringValue(), property.stringValue(), null, kg2Map.get(entity).get(property)));
                    } else if (!kg1Map.get(entity).get(property).equals(kg2Map.get(entity).get(property))) {
                        // Property exists but with a different value => modification
                        changes.add(new Change(Change.ChangeType.MODIFICATION, entity.stringValue(), property.stringValue(), kg1Map.get(entity).get(property), kg2Map.get(entity).get(property)));
                    }
                }
            }
        }

        // Detect deletions in kg1 that are missing in kg2
        for (Resource entity : kg1Map.keySet()) {
            if (!kg2Map.containsKey(entity)) {
                // If the entity is missing in kg2, all its properties are deletions
                for (IRI property : kg1Map.get(entity).keySet()) {
                    changes.add(new Change(Change.ChangeType.DELETION, entity.stringValue(), property.stringValue(), kg1Map.get(entity).get(property), null));
                }
            } else {
                // Check each property of the existing entity for deletions
                for (IRI property : kg1Map.get(entity).keySet()) {
                    if (!kg2Map.get(entity).containsKey(property)) {
                        // Property was removed from an existing entity
                        changes.add(new Change(Change.ChangeType.DELETION, entity.stringValue(), property.stringValue(), kg1Map.get(entity).get(property), null));
                    }
                }
            }
        }

        generateChangesReport(changes, Main.outputFilePath + Main.datasetName + "_DetectedChanges.txt");
        return changes;
    }

    /**
     * Converts an RDF4J model into a simplified map-based structure for efficient comparison.
     */
    private Map<Resource, Map<IRI, String>> convertToSimpleMap(Model model) {
        Map<Resource, Map<IRI, String>> map = new ConcurrentHashMap<>();

        for (Statement stmt : model) {
            Resource subject = stmt.getSubject();
            IRI predicate = stmt.getPredicate();
            Value object = stmt.getObject();

            map.computeIfAbsent(subject, k -> new ConcurrentHashMap<>())
                    .put(predicate, object.stringValue());
        }

        return map;
    }

    /**
     * Converts detected changes into an N-Triples format for easier processing.
     */
    public String getChangesAsNTriples(List<Change> changes) {
        Model changeGraph = generateChangeGraph(changes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Rio.write(changeGraph, outputStream, RDFFormat.NTRIPLES);
        return outputStream.toString();
    }

    /**
     * Generate an RDF change graph from detected changes.
     */
    public Model generateChangeGraph(List<Change> changes) {
        Model changeGraph = new LinkedHashModel();
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

        for (Change change : changes) {
            IRI entity = valueFactory.createIRI(change.entity);
            IRI property = valueFactory.createIRI(change.property);

            switch (change.type) {
                case ADDITION:
                case MODIFICATION:
                    addLiteral(changeGraph, entity, property, change.newValue, valueFactory);
                    break;
                case DELETION:
                    addLiteral(changeGraph, entity, property, change.oldValue, valueFactory);
                    break;
            }
        }

        return changeGraph;
    }

    /**
     * Adds a literal or IRI value to the change graph.
     */
    private void addLiteral(Model changeGraph, IRI entity, IRI property, String value, ValueFactory valueFactory) {
        if (value != null) {
            if (value.startsWith("\"") && value.contains("^^")) {
                String literalValue = value.substring(1, value.lastIndexOf("\""));
                String datatype = value.substring(value.lastIndexOf("^^<") + 3, value.length() - 1);
                IRI datatypeIRI = valueFactory.createIRI(datatype);
                changeGraph.add(entity, property, valueFactory.createLiteral(literalValue, datatypeIRI));
            } else if (value.startsWith("\"") && value.endsWith("\"")) {
                String literalValue = value.substring(1, value.length() - 1);
                changeGraph.add(entity, property, valueFactory.createLiteral(literalValue));
            } else {
                try {
                    IRI iriValue = valueFactory.createIRI(value);
                    changeGraph.add(entity, property, iriValue);
                } catch (IllegalArgumentException e) {
                    System.err.println("Invalid IRI: " + value);
                }
            }
        }
    }

    /**
     * Save the detected changes to a file.
     */
    public void generateChangesReport(List<Change> changes, String outputFilePath) {
        Map<Change.ChangeType, List<Change>> groupedChanges = new HashMap<>();
        for (Change.ChangeType type : Change.ChangeType.values()) {
            groupedChanges.put(type, new ArrayList<>());
        }

        for (Change change : changes) {
            groupedChanges.get(change.type).add(change);
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            int totalCount = changes.size();
            writer.println("Total Changes (" + totalCount + "):\n");

            List<Change> additions = groupedChanges.get(Change.ChangeType.ADDITION);
            List<Change> modifications = groupedChanges.get(Change.ChangeType.MODIFICATION);
            List<Change> deletions = groupedChanges.get(Change.ChangeType.DELETION);
            writer.println("Detected (" + additions.size() + ") Added Triples:");
            writer.println("Detected (" + modifications.size() + ") Modified Triples:");
            writer.println("Detected (" + deletions.size() + ") Removed Triples:");

            writer.println("Detected (" + additions.size() + ") Added Triples:");
            for (Change change : additions) {
                writer.println("Added Triple: <" + change.entity + "> <" + change.property + "> <" + change.newValue + "> .");
            }
            writer.println();

            writer.println("Detected (" + modifications.size() + ") Modified Triples:");
            for (Change change : modifications) {
                if (change.oldValue != null && change.newValue != null) {
                    writer.println("Modified Triple: <" + change.entity + "> <" + change.property + "> <" + change.newValue + "> . - Object Updated From <" + change.oldValue + "> To <" + change.newValue + ">");
                } else {
                    writer.println("Modified Triple: <" + change.entity + "> <" + change.oldValue + "> <" + change.newValue + "> . - Predicate Updated From <" + change.oldValue + "> To <" + change.newValue + ">");
                }
            }
            writer.println();

            writer.println("Detected (" + deletions.size() + ") Removed Triples:");
            for (Change change : deletions) {
                writer.println("Removed Triple: <" + change.entity + "> <" + change.property + "> <" + change.oldValue + "> .");
            }
            writer.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
