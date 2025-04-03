package cs.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.*;
import org.apache.jena.shacl.*;
import org.apache.jena.shacl.validation.ReportEntry;
import org.apache.jena.vocabulary.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SHACLValidator {

    private static final String INTEGER_TYPE_URI = "http://www.w3.org/2001/XMLSchema#integer";
    private static final String INT_TYPE_URI = "http://www.w3.org/2001/XMLSchema#int";

    // Validate the given data model against the SHACL shapes model.
    public List<ReportEntry> validateModel(Model dataModel, Model shapesModel) {
        List<ReportEntry> violations = new ArrayList<>();
        try {
            SHACLCleaner cleaner = new SHACLCleaner();
            Model cleanedShapesModel = cleaner.cleanSHACLShapes(shapesModel);
            Shapes shapes = Shapes.parse(cleanedShapesModel);
            ValidationReport report = ShaclValidator.get().validate(shapes, dataModel.getGraph());
            violations.addAll(report.getEntries());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return violations;
    }

    // Validate the data model in parallel and process violations immediately.
    public void validateModelInParallelAndProcessDirectly(Model dataModel, Shapes shapes, AtomicInteger violationCount,
                                                          Model updatedGraph, Model deltaGraph, Set<Resource> visitedNodes) {
        if (dataModel == null || shapes == null) {
            throw new IllegalArgumentException("Data model or shapes cannot be null.");
        }
        List<ReportEntry> violations = new ArrayList<>();
        try {
            List<Resource> subjects = dataModel.listSubjects().toList();
            int totalSubjects = subjects.size();
            int batchSize = Math.min(10000, totalSubjects / Runtime.getRuntime().availableProcessors());
            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            List<Future<Void>> futures = new ArrayList<>();
            for (int i = 0; i < totalSubjects; i += batchSize) {
                List<Resource> batch = subjects.subList(i, Math.min(i + batchSize, totalSubjects));
                futures.add(executor.submit(() -> {
                    validateBatch(batch, shapes, dataModel, violations, violationCount, updatedGraph, deltaGraph, visitedNodes);
                    return null;
                }));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void validateBatch(List<Resource> batch, Shapes shapes, Model dataModel, List<ReportEntry> violations,
                               AtomicInteger violationCount, Model updatedGraph, Model deltaGraph, Set<Resource> visitedNodes) {
        for (Resource subject : batch) {
            try {
                Model subjectModel = ModelFactory.createDefaultModel();
                StmtIterator stmtIter = dataModel.listStatements(subject, null, (RDFNode) null);
                while (stmtIter.hasNext()) {
                    subjectModel.add(stmtIter.next());
                }
                ValidationReport report = ShaclValidator.get().validate(shapes, subjectModel.getGraph());
                synchronized (violations) {
                    for (ReportEntry entry : report.getEntries()) {
                        violations.add(entry);
                        violationCount.incrementAndGet();
                        // Copy only triples from Vn+1 in which the violating focus node appears as a subject.
                        processViolationInMemory(entry, updatedGraph, deltaGraph, visitedNodes, false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Copy from Vn+1 only those triples where the violating focus node appears as a subject.
    private static void processViolationInMemory(ReportEntry entry, Model updatedGraph,
                                                 Model deltaGraph, Set<Resource> visitedNodes,
                                                 boolean copyObjects) {
        Node focusNode = entry.focusNode();
        if (focusNode.isURI()) {
            Resource resourceFocusNode = updatedGraph.createResource(focusNode.getURI());
            if (visitedNodes.add(resourceFocusNode)) {
                // Copy triples where the focus node is the subject.
                StmtIterator iter = updatedGraph.listStatements(resourceFocusNode, null, (RDFNode) null);
                while (iter.hasNext()) {
                    deltaGraph.add(iter.next());
                }
                // Optionally copy triples where it appears as object.
                if (copyObjects) {
                    StmtIterator iterObj = updatedGraph.listStatements(null, null, resourceFocusNode);
                    while (iterObj.hasNext()) {
                        deltaGraph.add(iterObj.next());
                    }
                }
            }
        }
    }
}
