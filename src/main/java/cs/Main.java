package cs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import cs.cose.ChangeDetection;
import cs.cose.ChangeDetection.Change;
import cs.cose.Parser;
import cs.cose.ShapesMerger;
import cs.cose.SubgraphExtraction;
import cs.utils.ConfigManager;
import cs.utils.FilesUtil;
import cs.utils.GraphLoader;
import cs.utils.SHACLValidator;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.shacl.validation.ReportEntry;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class Main {
    public static String configPath;
    public static String datasetName;
    public static boolean extractMaxCardConstraints;
    public static boolean shapesFromSpecificClasses;
    public static String datasetPath;
    public static String outputFilePath;
    public static String originalGraphPath;
    public static String DeltaShapePath;
    public static String originalShapePath;

    public static void main(String[] args) {
        configPath = args[0];

        try {
            // Load configuration properties
            datasetPath = ConfigManager.getProperty("dataset_path");
            originalGraphPath = ConfigManager.getProperty("originalGraphPath");
            originalShapePath = ConfigManager.getProperty("SHAPES_FILE_PATH");
            outputFilePath = ConfigManager.getProperty("output_file_path");
            datasetName = FilesUtil.getFileName(datasetPath);
            DeltaShapePath = outputFilePath + datasetName + "_QSE_FULL_SHACL.ttl";
            extractMaxCardConstraints = Boolean.parseBoolean(ConfigManager.getProperty("extractMaxCardConstraints"));
            shapesFromSpecificClasses = Boolean.parseBoolean(ConfigManager.getProperty("shapesFromSpecificClasses"));

            int totalLines = countLines(datasetPath);
            int SAMPLE_SIZE = (int) Math.ceil(totalLines * 0.1);  

            String MergedShapesPath = outputFilePath + datasetName + "_Merged_SHACL.ttl";

            // Load original shape and graph models
            Model jenaShapesModel = loadJenaModel(originalShapePath, Lang.TURTLE);
            org.eclipse.rdf4j.model.Model originalGraph = GraphLoader.loadRDF4JModel(originalGraphPath);
            org.eclipse.rdf4j.model.Model updatedGraph = GraphLoader.loadRDF4JModel(datasetPath);

            if (jenaShapesModel.isEmpty()) {
                System.err.println("Shapes model is empty. Please provide valid shapes.");
                return;
            }

            // Detect changes between original and updated graphs
            ChangeDetection changeDetection = new ChangeDetection();
            List<Change> changes = changeDetection.detectChanges(originalGraph, updatedGraph);
            String nTriplesChangeGraph = changeDetection.getChangesAsNTriples(changes);
            Model jenaChangeGraph = convertNTriplesToJenaModel(nTriplesChangeGraph);

            // Validate the detected changes against the SHACL shapes
            SHACLValidator validator = new SHACLValidator();
            List<ReportEntry> violations = validator.validateModel(jenaChangeGraph, jenaShapesModel);

            if (violations.isEmpty()) {
                System.out.println("Data conforms to SHACL shapes.");
            } else {
                System.out.println("Start Graph Extraction");
                // Extract relevant subgraph based on violations and SAMPLE_SIZE
                Model relevantSubgraph = SubgraphExtraction.extractRelevantSubgraph(jenaChangeGraph, violations, changes, jenaShapesModel, SAMPLE_SIZE);
                System.out.println("Extracted DeltaGraph contains " + relevantSubgraph.size() + " statements.");

                org.eclipse.rdf4j.model.Model rdf4jSubgraph = convertJenaModelToRDF4JModel(relevantSubgraph);
                Parser parser = new Parser(rdf4jSubgraph, 1000, 10000, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
                parser.run();

                org.eclipse.rdf4j.model.Model extractedShapes = GraphLoader.loadRDF4JModel(DeltaShapePath);
                org.eclipse.rdf4j.model.Model jenaShapesRDF4JModel = convertJenaModelToRDF4JModel(jenaShapesModel);

                // Merge the shapes using ShapesMerger
                ShapesMerger shapesMerger = new ShapesMerger(jenaShapesRDF4JModel);
                shapesMerger.merge(originalShapePath, DeltaShapePath, MergedShapesPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Error parsing configuration value: " + e.getMessage());
        }
    }

    // Method to count the lines in the dataset file
    private static int countLines(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int lines = 0;
            while (reader.readLine() != null) lines++;
            return lines;
        }
    }

    private static Model loadJenaModel(String filePath, Lang lang) throws IOException {
        Model model = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
        RDFParser.source(filePath).lang(lang).parse(model.getGraph());
        return model;
    }

    private static Model convertNTriplesToJenaModel(String nTriples) {
        Model model = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
        RDFParser.source(new ByteArrayInputStream(nTriples.getBytes()))
                .lang(Lang.NTRIPLES)
                .parse(model.getGraph());
        return model;
    }

    private static org.eclipse.rdf4j.model.Model convertJenaModelToRDF4JModel(Model jenaModel) {
        org.eclipse.rdf4j.model.Model rdf4jModel = new LinkedHashModel();
        jenaModel.listStatements().forEachRemaining(stmt -> {
            try {
                org.eclipse.rdf4j.model.Resource subject = stmt.getSubject().isURIResource() ?
                        Values.iri(stmt.getSubject().getURI()) :
                        Values.bnode(stmt.getSubject().getId().getLabelString());
                IRI predicate = Values.iri(stmt.getPredicate().getURI());
                Value object;
                if (stmt.getObject().isURIResource()) {
                    object = Values.iri(stmt.getObject().asResource().getURI());
                } else if (stmt.getObject().isAnon()) {
                    object = Values.bnode(stmt.getObject().asResource().getId().getLabelString());
                } else if (stmt.getObject().isLiteral()) {
                    if (stmt.getObject().asLiteral().getDatatypeURI() != null) {
                        object = Values.literal(stmt.getObject().asLiteral().getLexicalForm(), Values.iri(stmt.getObject().asLiteral().getDatatypeURI()));
                    } else {
                        object = Values.literal(stmt.getObject().asLiteral().getLexicalForm());
                    }
                } else {
                    object = Values.literal(stmt.getObject().toString());
                }
                rdf4jModel.add(subject, predicate, object);
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid statement found and skipped: " + e.getMessage());
            }
        });
        return rdf4jModel;
    }
}
