package cs.cose;

import cs.Main;
import cs.utils.SupportConfidence;
import cs.utils.TSSSampler;
import cs.utils.Tuple3;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shacl.validation.ReportEntry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DeltagraphExtraction {

    public static Model extractDeltaGraph(
            Model updatedGraph,
            Model originalGraph,
            List<ReportEntry> violations,
            Model shapes,
            int sampleSize,
            double alpha,  // e.g., 0.15
            int kmin,      // e.g., 5000
            Map<Integer, Map<Integer, Set<Integer>>> cpot,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts
    ) {
        int availableThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(availableThreads);
        List<Future<?>> futures = new ArrayList<>();
        Set<Resource> visitedNodes = ConcurrentHashMap.newKeySet();
        Model deltaGraph = ModelFactory.createDefaultModel();
        TSSSampler tssSampler = new TSSSampler(cpot, alpha, kmin);
        AtomicInteger sampledCount = new AtomicInteger(0);

        // Process violations: copy only triples where the violating node appears as a subject.
        processViolationsInMemory(violations, updatedGraph, deltaGraph, tssSampler, visitedNodes, sampledCount);

        // Perform batch sampling in parallel.
        performBatchSampling(sampleSize, originalGraph, deltaGraph, tssSampler, visitedNodes, futures, executorService, sampledCount, availableThreads);

        waitForCompletion(futures);
        executorService.shutdown();

        System.out.println("Triples in Updated Graph: " + updatedGraph.size());
        System.out.println("Sampled nodes: " + sampledCount.get());
        System.out.println("Visited nodes: " + visitedNodes.size());
        System.out.println("Triples in Delta Graph: " + deltaGraph.size());

        saveDeltaGraphToFile(deltaGraph, Main.outputFilePath + Main.datasetName + "_DeltaGraph.nt");

        return deltaGraph;
    }

    private static void processViolationsInMemory(
            List<ReportEntry> violations,
            Model updatedGraph,
            Model deltaGraph,
            TSSSampler tssSampler,
            Set<Resource> visitedNodes,
            AtomicInteger sampledCount
    ) {
        // Use parallelStream for performance but synchronize writes to deltaGraph.
        violations.parallelStream().forEach(entry -> {
            Node focusNode = entry.focusNode();
            if (focusNode.isURI()) {
                Resource resourceFocusNode = updatedGraph.createResource(focusNode.getURI());
                if (visitedNodes.add(resourceFocusNode)) {
                    // Collect statements where this node is the subject.
                    List<Statement> stmts = new ArrayList<>();
                    StmtIterator subjIter = updatedGraph.listStatements(resourceFocusNode, null, (RDFNode) null);
                    while (subjIter.hasNext()) {
                        stmts.add(subjIter.next());
                    }
                    // Synchronized block to prevent concurrent writes.
                    synchronized (deltaGraph) {
                        deltaGraph.add(stmts);
                    }
                    // Add additional relevant statements using the sampler.
                    tssSampler.addRelevantStatements(resourceFocusNode, updatedGraph, deltaGraph);
                    sampledCount.incrementAndGet();
                }
            }
        });
    }

    private static void performBatchSampling(
            int threshold,
            Model originalGraph,
            Model deltaGraph,
            TSSSampler tssSampler,
            Set<Resource> visitedNodes,
            List<Future<?>> futures,
            ExecutorService executorService,
            AtomicInteger sampledCount,
            int availableThreads
    ) {
        List<Resource> subjects = originalGraph.listSubjects().toList();
        Random random = new Random();
        int batchSize = Math.max(1, subjects.size() / availableThreads);

        for (int i = 0; i < availableThreads; i++) {
            int startIndex = i * batchSize;
            int endIndex = Math.min(subjects.size(), (i + 1) * batchSize);
            futures.add(executorService.submit(() -> {
                for (int j = startIndex; j < endIndex; j++) {
                    if (visitedNodes.size() >= threshold || sampledCount.get() >= threshold)
                        break;
                    Resource randomEntity = subjects.get(random.nextInt(subjects.size()));
                    if (visitedNodes.add(randomEntity)) {
                        tssSampler.sample(randomEntity, originalGraph, deltaGraph, visitedNodes);
                        sampledCount.incrementAndGet();
                    }
                }
            }));
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

    private static void saveDeltaGraphToFile(Model deltaGraph, String filePath) {
        try (OutputStream out = new FileOutputStream(filePath)) {
            RDFDataMgr.write(out, deltaGraph, org.apache.jena.riot.Lang.NTRIPLES);
            System.out.println("Delta Graph saved to: " + filePath);
        } catch (IOException e) {
            throw new RuntimeException("Error saving Delta Graph to file.", e);
        }
    }
}
