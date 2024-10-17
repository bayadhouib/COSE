package cs.cose;

import cs.Main;
import cs.utils.ReservoirSampler;
import cs.utils.Utils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shacl.validation.ReportEntry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SubgraphExtraction {

    public static Model extractRelevantSubgraph(Model updatedGraph, List<ReportEntry> violations, List<ChangeDetection.Change> changes, Model shapes, int sampleSize) {
        StopWatch watch = new StopWatch();
        watch.start();

        Model subgraph = ModelFactory.createDefaultModel();
        Set<Resource> visitedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<?>> futures = new ArrayList<>();

        AtomicInteger violationCount = new AtomicInteger(0);
        for (ReportEntry violation : violations) {
            futures.add(executorService.submit(() -> {
                Resource focusNode = violation.focusNode().isURI() ?
                        ResourceFactory.createResource(violation.focusNode().getURI()) :
                        ResourceFactory.createResource(violation.focusNode().getBlankNodeId().getLabelString());

                if (visitedNodes.add(focusNode)) {
                    addRelevantStatements(focusNode, updatedGraph, subgraph, visitedNodes);
                }
                violationCount.incrementAndGet();
            }));
        }

        AtomicInteger changeCount = new AtomicInteger(0);
        for (ChangeDetection.Change change : changes) {
            futures.add(executorService.submit(() -> {
                if (change.newValue != null && isChangeRelatedToViolation(change, violations)) {
                    Resource changeEntity = ResourceFactory.createResource(change.entity);

                    if (visitedNodes.add(changeEntity)) {
                        addRelevantStatements(changeEntity, updatedGraph, subgraph, visitedNodes);
                    }
                }
                changeCount.incrementAndGet();
            }));
        }

        AtomicInteger sampleCount = new AtomicInteger(0);
        List<Resource> sampledNodes = ReservoirSampler.standardReservoirSampling(updatedGraph, null, sampleSize);
        for (Resource sampledNode : sampledNodes) {
            futures.add(executorService.submit(() -> {
                if (visitedNodes.add(sampledNode)) {
                    addRelevantStatements(sampledNode, updatedGraph, subgraph, visitedNodes);
                }
                sampleCount.incrementAndGet();
            }));
        }

        waitForCompletion(futures);
        executorService.shutdown();

        saveSubgraphToFile(subgraph, Main.outputFilePath + Main.datasetName + "_DeltaGraph.nt");

        watch.stop();
        Utils.logTime("::: DeltaGraphExtraction", TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));

        return subgraph;
    }

    private static void addRelevantStatements(Resource entity, Model updatedGraph, Model subgraph, Set<Resource> visitedNodes) {
        StmtIterator stmtIterator = updatedGraph.listStatements(entity, null, (RDFNode) null);
        while (stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            synchronized (subgraph) {
                if (stmt.getSubject() != null && stmt.getPredicate() != null && stmt.getObject() != null) {
                    subgraph.add(stmt);
                }
            }
        }
        stmtIterator.close();
    }

    private static boolean isChangeRelatedToViolation(ChangeDetection.Change change, List<ReportEntry> violations) {
        return violations.parallelStream().anyMatch(violation -> {
            String focusNodeURI = violation.focusNode().isURI() ? violation.focusNode().getURI() : violation.focusNode().getBlankNodeId().getLabelString();
            String resultPathURI = violation.resultPath().toString();
            return change.entity.equals(focusNodeURI) && change.property.equals(resultPathURI);
        });
    }

    private static void saveSubgraphToFile(Model subgraph, String filePath) {
        File file = new File(filePath);
        try (OutputStream out = new FileOutputStream(file)) {
            RDFDataMgr.write(out, subgraph, Lang.NTRIPLES);
            out.flush();
            System.out.println("::: DeltaGraphExtractor ~ WRITING Graph TO FILE: " + file.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void waitForCompletion(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
