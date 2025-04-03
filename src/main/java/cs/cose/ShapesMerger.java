package cs.cose;

import cs.utils.SupportConfidence;
import cs.utils.Tuple3;
import cs.utils.Utils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ShapesMerger {

    private final Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupportS1;
    private final Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupportSDelta;
    private final ValueFactory valueFactory;

    public ShapesMerger(
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupportS1,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupportSDelta) {
        this.shapeTripletSupportS1 = shapeTripletSupportS1;
        this.shapeTripletSupportSDelta = shapeTripletSupportSDelta;
        this.valueFactory = SimpleValueFactory.getInstance();
    }

    public Model merge(String originalShapePath, String deltaShapePath, String runtimeLogPath) {
        Model originalModel = loadModel(originalShapePath);
        Model deltaModel = loadModel(deltaShapePath);

        if (deltaModel.isEmpty()) {
            return originalModel;
        }

        cleanModel(originalModel);
        cleanModel(deltaModel);

        Model mergedModel = new LinkedHashModel();

        for (Resource shape : deltaModel.subjects()) {
            if (originalModel.contains(shape, null, null)) {
                mergeConstraints(shape, originalModel, deltaModel, mergedModel);
            } else {
                addConstraintsWithAnnotations(deltaModel.filter(shape, null, null), mergedModel);
            }
        }

        return mergedModel;
    }

    private void mergeConstraints(Resource shape, Model originalModel, Model deltaModel, Model mergedModel) {
        Model originalShapeModel = originalModel.filter(shape, null, null);
        Model deltaShapeModel = deltaModel.filter(shape, null, null);

        for (Statement deltaStmt : deltaShapeModel) {
            if (originalShapeModel.contains(deltaStmt)) {
                continue;
            }
            if (originalShapeModel.filter(deltaStmt.getSubject(), deltaStmt.getPredicate(), null).isEmpty()) {
                addConstraintWithAnnotation(deltaStmt, mergedModel);
            } else {
                resolveConflict(deltaStmt, originalShapeModel, mergedModel);
            }
        }
    }

    private void resolveConflict(Statement deltaStmt, Model originalShapeModel, Model mergedModel) {
        Tuple3<Integer, Integer, Integer> deltaKey = extractKey(deltaStmt);
        Statement originalStmt = originalShapeModel.filter(deltaStmt.getSubject(), deltaStmt.getPredicate(), null).iterator().next();
        Tuple3<Integer, Integer, Integer> originalKey = extractKey(originalStmt);

        SupportConfidence deltaSC = shapeTripletSupportSDelta.getOrDefault(deltaKey, new SupportConfidence(0, 0.0));
        SupportConfidence originalSC = shapeTripletSupportS1.getOrDefault(originalKey, new SupportConfidence(0, 0.0));

        SupportConfidence normalizedDeltaSC = normalizeSupportConfidence(deltaSC);
        // If the difference is negligible, choose the original constraint.
        Statement chosenStmt = Math.abs(normalizedDeltaSC.compareTo(originalSC)) < 1 ? originalStmt :
                (normalizedDeltaSC.compareTo(originalSC) > 0 ? deltaStmt : originalStmt);
        addConstraintWithAnnotation(chosenStmt, mergedModel);
    }

    private void addConstraintsWithAnnotations(Model constraints, Model mergedModel) {
        for (Statement stmt : constraints) {
            addConstraintWithAnnotation(stmt, mergedModel);
        }
    }

    private void addConstraintWithAnnotation(Statement stmt, Model mergedModel) {
        mergedModel.add(stmt);
        Tuple3<Integer, Integer, Integer> key = extractKey(stmt);
        SupportConfidence sc = shapeTripletSupportS1.getOrDefault(key, null);
        if (sc == null) {
            sc = shapeTripletSupportSDelta.getOrDefault(key, new SupportConfidence(0, 0.0));
        }
        if (sc != null) {
            Resource subject = stmt.getSubject();
            mergedModel.add(subject, valueFactory.createIRI("http://example.org/support"), valueFactory.createLiteral(sc.getSupport()));
            mergedModel.add(subject, valueFactory.createIRI("http://example.org/confidence"), valueFactory.createLiteral(sc.getConfidence()));
        }
    }

    private SupportConfidence normalizeSupportConfidence(SupportConfidence sc) {
        double normalizedSupport = sc.getSupport() * 0.8;
        return new SupportConfidence((int) normalizedSupport, sc.getConfidence());
    }

    private void cleanModel(Model model) {
        model.filter(null, SHACL.PATH, null)
                .stream()
                .collect(Collectors.groupingBy(Statement::getSubject))
                .forEach((shape, paths) -> {
                    if (paths.size() > 1) {
                        Value retainedPath = paths.get(0).getObject();
                        paths.forEach(model::remove);
                        model.add(shape, SHACL.PATH, retainedPath);
                    }
                });
    }

    private Tuple3<Integer, Integer, Integer> extractKey(Statement stmt) {
        int subject = stmt.getSubject().stringValue().hashCode();
        int predicate = stmt.getPredicate().stringValue().hashCode();
        int object = stmt.getObject().stringValue().hashCode();
        return new Tuple3<>(subject, predicate, object);
    }

    private Model loadModel(String filePath) {
        try (FileInputStream input = new FileInputStream(filePath)) {
            return Rio.parse(input, "", RDFFormat.TURTLE);
        } catch (IOException | RDFParseException e) {
            System.err.println("Error loading model from: " + filePath + " - " + e.getMessage());
            return new LinkedHashModel();
        }
    }

    public void saveMergedModel(Model mergedModel, String outputPath) {
        try (FileOutputStream output = new FileOutputStream(outputPath)) {
            Rio.write(mergedModel, output, RDFFormat.TURTLE);
            System.out.println("Merged SHACL shapes saved to: " + outputPath);
        } catch (IOException e) {
            System.err.println("Error saving merged model to: " + outputPath);
        }
    }
}
