package cs.cose;

import cs.Main;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class ShapesMerger {

    private Model referenceModel;

    public ShapesMerger(Model referenceModel) {
        this.referenceModel = referenceModel;
    }

   
    public void merge(String originalShapePath, String deltaShapePath, String outputFilePath) {
        Model originalModel = loadModel(originalShapePath);
        Model deltaModel = loadModel(deltaShapePath);

        cleanModel(originalModel);
        cleanModel(deltaModel);
        Model mergedModel = new LinkedHashModel();
        iterativeMerge(originalModel, deltaModel, mergedModel);
        resolvePathConflicts(mergedModel);
        saveModelStreaming(mergedModel, outputFilePath);

        analyzeMissingShapes(mergedModel);
    }

   
    private Model loadModel(String filePath) {
        Model model = new LinkedHashModel();
        try (FileInputStream input = new FileInputStream(filePath)) {
            model = Rio.parse(input, "", RDFFormat.TURTLE);
        } catch (IOException e) {
            System.err.println("Error loading model from: " + filePath);
            e.printStackTrace();
        }
        return model;
    }

   
    private void saveModelStreaming(Model model, String filePath) {
        try (OutputStream out = Files.newOutputStream(Path.of(filePath))) {
            RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, out);
            writer.startRDF();
            // Stream each statement to the file to avoid loading all into memory
            for (Statement st : model) {
                writer.handleStatement(st);
            }
            writer.endRDF();
            System.out.println("Merged shapes saved to: " +  Main.outputFilePath + Main.datasetName + "_Merged_SHACL.ttl");
        } catch (IOException e) {
            System.err.println("Error saving model to: " +  Main.outputFilePath + Main.datasetName + "_Merged_SHACL.ttl");
            e.printStackTrace();
        }
    }

    
    private void iterativeMerge(Model originalModel, Model deltaModel, Model mergedModel) {
        mergeModels(originalModel, mergedModel);

        for (Resource shape : deltaModel.subjects()) {
            if (shape instanceof IRI) {
                IRI shapeIRI = (IRI) shape;
                Model tempModel = deltaModel.filter(shapeIRI, null, null);

                if (isRelevant(tempModel)) {
                    mergeModels(tempModel, mergedModel);
                } else {
                    System.err.println("Excluded irrelevant shape: " + shapeIRI);
                }
            }
        }
    }

  
    private boolean isRelevant(Model shapeModel) {
        // Iterate through all subjects in the shape model
        for (Resource shape : shapeModel.subjects()) {
            // Check if the reference model contains this shape as a subject
            if (referenceModel.contains(shape, null, null)) {
                // Collect constraints from the delta model
                Set<IRI> deltaPaths = collectProperty(shapeModel, shape, SHACL.PATH);
                Set<IRI> deltaDataTypes = collectProperty(shapeModel, shape, SHACL.DATATYPE);
                Set<Literal> deltaMinCounts = collectLiteralProperty(shapeModel, shape, SHACL.MIN_COUNT);
                Set<Literal> deltaMaxCounts = collectLiteralProperty(shapeModel, shape, SHACL.MAX_COUNT);

                // Collect corresponding constraints from the reference model
                Set<IRI> referencePaths = collectProperty(referenceModel, shape, SHACL.PATH);
                Set<IRI> referenceDataTypes = collectProperty(referenceModel, shape, SHACL.DATATYPE);
                Set<Literal> referenceMinCounts = collectLiteralProperty(referenceModel, shape, SHACL.MIN_COUNT);
                Set<Literal> referenceMaxCounts = collectLiteralProperty(referenceModel, shape, SHACL.MAX_COUNT);

                // Check alignment for each constraint type
                boolean pathMatches = !Collections.disjoint(deltaPaths, referencePaths);
                boolean dataTypeMatches = !Collections.disjoint(deltaDataTypes, referenceDataTypes);
                boolean minCountMatches = !Collections.disjoint(deltaMinCounts, referenceMinCounts);
                boolean maxCountMatches = !Collections.disjoint(deltaMaxCounts, referenceMaxCounts);

                // Return true if any constraint matches, indicating relevance
                if (pathMatches || dataTypeMatches || minCountMatches || maxCountMatches) {
                    return true;
                }
            }
        }
        return false; 
    }

    /**
     * Helper method to collect properties of a specific type as IRIs.
     */
    private Set<IRI> collectProperty(Model model, Resource shape, IRI property) {
        return model.filter(shape, property, null)
                .objects()
                .stream()
                .filter(obj -> obj instanceof IRI)
                .map(obj -> (IRI) obj)
                .collect(Collectors.toSet());
    }

    /**
     * Merges SHACL node shapes by combining properties and resolving conflicts with the most restrictive constraints.
     */
    private void mergeNodeShapes(Model sourceNodeModel, Model targetModel) {
        List<Statement> toAdd = new ArrayList<>();
        List<Statement> toRemove = new ArrayList<>();

        for (Resource nodeShape : sourceNodeModel.subjects()) {
            // Check if the node shape is already contained in the target model
            if (targetModel.contains(nodeShape, RDF.TYPE, SHACL.NODE_SHAPE)) {
                Model existingShapeModel = targetModel.filter(nodeShape, null, null);

                // Iterate over each property in the source node shape and merge
                sourceNodeModel.filter(nodeShape, SHACL.PROPERTY, null).objects().stream()
                        .filter(obj -> obj instanceof Resource)
                        .forEach(property -> {
                            Resource propertyShape = (Resource) property;
                            mergeProperties(propertyShape, existingShapeModel, sourceNodeModel, toAdd, toRemove);
                        });
            } else {
                // If the node shape is not in the target, add it directly
                sourceNodeModel.filter(nodeShape, null, null).forEach(toAdd::add);
            }
        }

        // Update the target model by removing old statements and adding new ones
        targetModel.removeAll(toRemove);
        targetModel.addAll(toAdd);
    }

    /**
     * Merges properties of SHACL node shapes, resolving conflicts with the most restrictive values.
     */
    private void mergeProperties(Resource propertyShape, Model existingShapeModel, Model sourceNodeModel, List<Statement> toAdd, List<Statement> toRemove) {
        // Remove existing property constraints for the shape to be replaced with updated constraints
        Model tempModel = existingShapeModel.filter(propertyShape, null, null);
        tempModel.forEach(toRemove::add);

        List<Statement> sourceStatements = new ArrayList<>(sourceNodeModel.filter(propertyShape, null, null));
        IRI retainedPath = null;

        for (Statement stmt : sourceStatements) {
            IRI predicate = stmt.getPredicate();

            if (predicate.equals(SHACL.PATH)) {
                if (retainedPath == null) {
                    retainedPath = (IRI) stmt.getObject();
                } else if (!retainedPath.equals(stmt.getObject())) {
                    System.err.println("Conflicting sh:path values detected for property shape: " + propertyShape + ". Retaining only: " + retainedPath);
                }
            } else {
                toAdd.add(stmt); // Add the other constraints
            }
        }

        // Add back the retained sh:path to avoid conflicts
        if (retainedPath != null) {
            toAdd.add(SimpleValueFactory.getInstance().createStatement(propertyShape, SHACL.PATH, retainedPath));
        }

        toRemove.forEach(existingShapeModel::remove);
        toAdd.forEach(existingShapeModel::add);
    }



    /**
     * Helper method to collect properties of a specific type as Literals (for numeric constraints).
     */
    private Set<Literal> collectLiteralProperty(Model model, Resource shape, IRI property) {
        return model.filter(shape, property, null)
                .objects()
                .stream()
                .filter(obj -> obj instanceof Literal)
                .map(obj -> (Literal) obj)
                .collect(Collectors.toSet());
    }

    /**
     * Merges shapes from the source model into the target model with containment checks.
     */
    private void mergeModels(Model sourceModel, Model targetModel) {
        for (Resource subject : sourceModel.subjects()) {
            if (subject instanceof IRI) {
                IRI subjectIRI = (IRI) subject;
                Model tempModel = sourceModel.filter(subjectIRI, null, null);
                if (!isContained(targetModel, tempModel)) {
                    if (targetModel.contains(subjectIRI, RDF.TYPE, SHACL.NODE_SHAPE)) {
                        // If the shape exists, merge the properties
                        mergeNodeShapes(tempModel, targetModel);
                    } else {
                        // Otherwise, add the entire shape
                        targetModel.addAll(tempModel);
                    }
                }
            }
        }
    }

  
    private void analyzeMissingShapes(Model mergedModel) {
        Set<Resource> missingShapes = referenceModel.subjects().stream()
                .filter(shape -> !mergedModel.contains(shape, null, null))
                .collect(Collectors.toSet());

        for (Resource missingShape : missingShapes) {
            System.err.println("Missing shape from KG_Merged: " + missingShape);
        }
    }

    /**
     * Cleans the model by ensuring each property shape has only one sh:path before processing.
     */
    private void cleanModel(Model model) {
        Map<Resource, List<Statement>> problemShapes = model.filter(null, SHACL.PATH, null).stream()
                .collect(Collectors.groupingBy(Statement::getSubject));

        for (Map.Entry<Resource, List<Statement>> entry : problemShapes.entrySet()) {
            Resource shape = entry.getKey();
            List<Statement> paths = entry.getValue();

            if (paths.size() > 1) {
                Value retainedPath = paths.get(0).getObject();
                paths.forEach(model::remove);
                model.add(shape, SHACL.PATH, retainedPath);
                System.err.println("Cleaned multiple sh:path for shape: " + shape + ". Retained path: " + retainedPath);
            }
        }
    }

    /**
     * Checks and resolves multiple sh:path issues in the merged model.
     */
    private void resolvePathConflicts(Model model) {
        cleanModel(model);
        boolean conflictExists = model.filter(null, SHACL.PATH, null)
                .stream()
                .collect(Collectors.groupingBy(Statement::getSubject))
                .values()
                .stream()
                .anyMatch(statements -> statements.size() > 1);

        if (conflictExists) {
            throw new IllegalStateException("Failed to resolve all sh:path conflicts in the model.");
        }
    }

    /**
     * Checks if the source model's shape is already contained within the existing shape model.
     */
    private boolean isContained(Model existingShapeModel, Model sourceNodeModel) {
        for (Statement stmt : sourceNodeModel) {
            if (!existingShapeModel.contains(stmt.getSubject(), stmt.getPredicate(), stmt.getObject())) {
                return false;
            }
        }
        return true;
    }
}
