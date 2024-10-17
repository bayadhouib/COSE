package cs.utils;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class GraphLoader {

    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_POOL_SIZE = 8;

    public static Model loadRDF4JModel(String filePath) throws IOException {
        Model model = new LinkedHashModel();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<List<StatementWrapper>>> futures = new ArrayList<>();

        try (Stream<String> lines = Files.lines(Path.of(filePath))) {
            List<String> batch = new ArrayList<>(BATCH_SIZE);
            AtomicLong lineNumber = new AtomicLong(0);

            lines.forEach(line -> {
                batch.add(line);
                if (batch.size() >= BATCH_SIZE) {
                    futures.add(processBatch(executor, batch, lineNumber));
                    batch.clear();
                }
            });

            // Process remaining lines
            if (!batch.isEmpty()) {
                futures.add(processBatch(executor, batch, lineNumber));
            }

            // Collect results
            for (Future<List<StatementWrapper>> future : futures) {
                try {
                    for (StatementWrapper statement : future.get()) {
                        try {
                            org.eclipse.rdf4j.model.Resource subject = toRdf4jResource(statement.getSubject());
                            org.eclipse.rdf4j.model.IRI predicate = toRdf4jIRI(statement.getPredicate());
                            org.eclipse.rdf4j.model.Value object = toRdf4jValue(statement.getObject());
                            model.add(subject, predicate, object);
                        } catch (Exception e) {
//                            System.err.println("Error adding statement to model: " + e.getMessage());
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
//                    System.err.println("Error processing batch: " + e.getMessage());
                }
            }
        } catch (IOException e) {
//            System.err.println("Error reading file: " + filePath + ". Error: " + e.getMessage());
            throw e;
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
//                System.err.println("Executor interrupted: " + e.getMessage());
            }
        }

        System.out.println("Finished processing file.");
        return model;
    }

    private static Future<List<StatementWrapper>> processBatch(ExecutorService executor, List<String> batch, AtomicLong lineNumber) {
        return executor.submit(() -> {
            List<StatementWrapper> statements = new ArrayList<>();
            for (String line : batch) {
                lineNumber.incrementAndGet();
                try {
                    Node[] nodes = NxParser.parseNodes(line);
                    if (nodes.length == 3) {
                        statements.add(new StatementWrapper(nodes[0], nodes[1], nodes[2]));
                    }
                } catch (ParseException e) {
//                    System.err.println("Skipping malformed triple at line " + lineNumber + ": " + line + ". Error: " + e.getMessage());
                } catch (Exception e) {
//                    System.err.println("Error processing line " + lineNumber + ": " + line + ". Error: " + e.getMessage());
                }
            }
            return statements;
        });
    }

    private static org.eclipse.rdf4j.model.Resource toRdf4jResource(Node node) {
        if (node instanceof org.semanticweb.yars.nx.Resource) {
            return org.eclipse.rdf4j.model.util.Values.iri(node.getLabel());
        } else {
            return org.eclipse.rdf4j.model.util.Values.bnode(node.getLabel());
        }
    }

    private static org.eclipse.rdf4j.model.IRI toRdf4jIRI(Node node) {
        return org.eclipse.rdf4j.model.util.Values.iri(node.getLabel());
    }

    private static org.eclipse.rdf4j.model.Value toRdf4jValue(Node node) {
        if (node instanceof org.semanticweb.yars.nx.Resource) {
            return org.eclipse.rdf4j.model.util.Values.iri(node.getLabel());
        } else if (node instanceof org.semanticweb.yars.nx.BNode) {
            return org.eclipse.rdf4j.model.util.Values.bnode(node.getLabel());
        } else if (node instanceof org.semanticweb.yars.nx.Literal) {
            org.semanticweb.yars.nx.Literal literal = (org.semanticweb.yars.nx.Literal) node;
            if (literal.getDatatype() != null) {
                return org.eclipse.rdf4j.model.util.Values.literal(literal.getLabel(), org.eclipse.rdf4j.model.util.Values.iri(literal.getDatatype().toString()));
            } else if (literal.getLanguageTag() != null) {
                return org.eclipse.rdf4j.model.util.Values.literal(literal.getLabel(), literal.getLanguageTag());
            } else {
                return org.eclipse.rdf4j.model.util.Values.literal(literal.getLabel());
            }
        } else {
            return org.eclipse.rdf4j.model.util.Values.literal(node.getLabel());
        }
    }

    private static class StatementWrapper {
        private final Node subject;
        private final Node predicate;
        private final Node object;

        public StatementWrapper(Node subject, Node predicate, Node object) {
            this.subject = subject;
            this.predicate = predicate;
            this.object = object;
        }

        public Node getSubject() {
            return subject;
        }

        public Node getPredicate() {
            return predicate;
        }

        public Node getObject() {
            return object;
        }
    }
}
