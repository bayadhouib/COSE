package cs;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import cs.cose.ChangeDetection;
import cs.cose.DeltagraphExtraction;
import cs.cose.ShapesExtractor;
import cs.cose.ShapesMerger;
import cs.cose.encoders.ConcurrentStringEncoder;
import cs.cose.encoders.Encoder;
import cs.utils.ConfigManager;
import cs.utils.FilesUtil;
import cs.utils.SHACLCleaner;
import cs.utils.SHACLValidator;
import cs.utils.StatsComputer;
import cs.utils.SupportConfidence;
import cs.utils.Tuple3;
import cs.utils.Utils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.validation.ReportEntry;
import org.apache.jena.vocabulary.RDF;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static String configPath;
    public static String datasetName;
    public static String datasetPath;
    public static String outputFilePath;
    public static String originalGraphPath;
    public static String deltaShapePath;
    public static String originalShapePath;
    public static String graphDataPath;
    public static int SAMPLE_SIZE;
    public static boolean useFullGraphs;
    public static boolean extractMaxCardConstraints = true;
    private static final Encoder resourceEncoder = new ConcurrentStringEncoder();

    public static final Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes = new ConcurrentHashMap<>();
    public static final Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts = new ConcurrentHashMap<>();
    public static final Map<Integer, Integer> classToEntityCount = new ConcurrentHashMap<>();
    private static Model model = ModelFactory.createDefaultModel();
    private static Kryo kryo;

    static {
        kryo = new Kryo();
        kryo.register(HashMap.class);
        kryo.register(ConcurrentHashMap.class);
        kryo.register(ConcurrentHashMap.KeySetView.class);
        kryo.register(ArrayList.class);
        kryo.register(HashSet.class);
        kryo.register(Tuple3.class);
        kryo.register(SupportConfidence.class);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.setRegistrationRequired(true);
    }

    public static void main(String[] args) {
        configPath = args[0];
        String runtimeLogPath = generateOutputFilePath("_runtime.log");
        long startTimeTotal = System.nanoTime();
        long startMemoryTotal = getMemoryUsage();

        try {
            long startTime = System.nanoTime();
            long startMemory = getMemoryUsage();
            loadConfig();
            Utils.logRuntime(runtimeLogPath, "Load Configuration", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

            startTime = System.nanoTime();
            startMemory = getMemoryUsage();
            boolean loadedFromKryo = initializeDataStructures(originalGraphPath, runtimeLogPath);
            Utils.logRuntime(runtimeLogPath, loadedFromKryo ? "Load Data Structures from Kryo" : "Parse Graph and Initialize Data Structures", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

            startTime = System.nanoTime();
            startMemory = getMemoryUsage();
            Model jenaShapesModel = RDFDataMgr.loadModel(originalShapePath, Lang.TURTLE);
            Utils.logRuntime(runtimeLogPath, "Load SHACL Shapes", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

            if (useFullGraphs) {
                processFullGraphs(runtimeLogPath, jenaShapesModel);
            } else {
                processIncrementalChanges(runtimeLogPath, jenaShapesModel);
            }

            long totalElapsedTime = calculateElapsedTime(startTimeTotal);
            long totalMemoryUsage = calculateMemoryUsage(startMemoryTotal);
            Utils.logRuntime(runtimeLogPath, "Total Runtime", totalElapsedTime, totalMemoryUsage);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered: " + e.getMessage());
        }
    }

    private static void processFullGraphs(String runtimeLogPath, Model jenaShapesModel) throws IOException {
        long startTime = System.nanoTime();
        long startMemory = getMemoryUsage();
        Model updatedGraph = RDFDataMgr.loadModel(datasetPath, Lang.NTRIPLES);
        Utils.logRuntime(runtimeLogPath, "Load Updated Full Graph", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        startTime = System.nanoTime();
        startMemory = getMemoryUsage();
        Map<Integer, Map<Integer, Set<Integer>>> newCpot = extractGraphData(updatedGraph);
        Map<Integer, Integer> newCec = extractEntityCount(updatedGraph);
        Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts = extractSupportConfidence(updatedGraph);

        String changesOutputPath = generateOutputFilePath("_changes.nt");
        ChangeDetection.detectChangesToFile(classToPropWithObjTypes, classToEntityCount, sts, newCpot, newCec, newSts, datasetPath, changesOutputPath);
        Utils.logRuntime(runtimeLogPath, "Detect Changes", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        startTime = System.nanoTime();
        startMemory = getMemoryUsage();
        SHACLValidator validator = new SHACLValidator();
        SHACLCleaner cleaner = new SHACLCleaner();
        Model cleanedShapesModel = cleaner.cleanSHACLShapes(jenaShapesModel);
        Model changesModel = RDFDataMgr.loadModel(changesOutputPath, Lang.NTRIPLES);
        AtomicInteger violationCount = new AtomicInteger(0);
        Set<Resource> visitedNodes = ConcurrentHashMap.newKeySet();
        Model deltaGraph = ModelFactory.createDefaultModel();

        List<ReportEntry> violations = validator.validateModel(changesModel, cleanedShapesModel);
        validator.validateModelInParallelAndProcessDirectly(changesModel, Shapes.parse(cleanedShapesModel), violationCount, changesModel, deltaGraph, visitedNodes);
        Utils.logRuntime(runtimeLogPath, "Validate Detected Changes", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        processDeltaGraph(runtimeLogPath, violations, jenaShapesModel, newCpot, newCec, newSts);
    }

    private static void processIncrementalChanges(String runtimeLogPath, Model jenaShapesModel) throws IOException {
        long startTime = System.nanoTime();
        long startMemory = getMemoryUsage();
        Map<Integer, Map<Integer, Set<Integer>>> newCpot = new ConcurrentHashMap<>();
        Map<Integer, Integer> newCec = new ConcurrentHashMap<>();
        Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts = new ConcurrentHashMap<>();

        System.out.println("before newSts size: " + newSts.size());
        parseGraphStreaming(datasetPath, newCpot, newCec, newSts);
        System.out.println("after size: " + newSts.size());

        Utils.logRuntime(runtimeLogPath, "Parse Updated Graph Incrementally", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        startTime = System.nanoTime();
        startMemory = getMemoryUsage();
        SHACLValidator validator = new SHACLValidator();
        SHACLCleaner cleaner = new SHACLCleaner();
        Model cleanedShapesModel = cleaner.cleanSHACLShapes(jenaShapesModel);
        AtomicInteger violationCount = new AtomicInteger(0);
        Set<Resource> visitedNodes = ConcurrentHashMap.newKeySet();
        Model updatedGraph = RDFDataMgr.loadModel(datasetPath, Lang.NTRIPLES);
        Model deltaGraph = ModelFactory.createDefaultModel();

        List<ReportEntry> violations = validator.validateModel(updatedGraph, cleanedShapesModel);
        validator.validateModelInParallelAndProcessDirectly(updatedGraph, Shapes.parse(cleanedShapesModel), violationCount, updatedGraph, deltaGraph, visitedNodes);
        Utils.logRuntime(runtimeLogPath, "Validate Incremental Changes", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        processDeltaGraph(runtimeLogPath, violations, jenaShapesModel, newCpot, newCec, newSts);
    }

    private static void processDeltaGraph(String runtimeLogPath, List<ReportEntry> violations, Model jenaShapesModel,
                                          Map<Integer, Map<Integer, Set<Integer>>> newCpot, Map<Integer, Integer> newCec,
                                          Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts) throws IOException {
        long startTime = System.nanoTime();
        long startMemory = getMemoryUsage();

        Model originalGraph = RDFDataMgr.loadModel(originalGraphPath, Lang.NTRIPLES);
        // Use sampling parameters: alpha=0.15 and kmin=5000.
        Model deltaGraph = DeltagraphExtraction.extractDeltaGraph(
                RDFDataMgr.loadModel(datasetPath, Lang.NTRIPLES),
                originalGraph, violations, jenaShapesModel,
                SAMPLE_SIZE, 0.15, 5000, classToPropWithObjTypes, sts);

        System.out.println("Triples in Delta Graph: " + deltaGraph.size());

        String deltaGraphOutputPath = generateOutputFilePath("_DeltaGraph.nt");
        Utils.saveModelToFile(deltaGraph, deltaGraphOutputPath, Lang.NTRIPLES);
        Utils.logRuntime(runtimeLogPath, "Generate Delta Graph", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        postDeltaGraphProcessing(runtimeLogPath, deltaGraph, newSts);
    }

    private static void postDeltaGraphProcessing(String runtimeLogPath, Model deltaGraph, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts) throws IOException {
        long startTime = System.nanoTime();
        long startMemory = getMemoryUsage();
        StatsComputer statsComputer = new StatsComputer();
        statsComputer.computeSupportConfidenceFromDeltaGraph(deltaGraph, classToEntityCount);
        Utils.logRuntime(runtimeLogPath, "Compute Support and Confidence", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        startTime = System.nanoTime();
        startMemory = getMemoryUsage();
        Encoder encoder = new ConcurrentStringEncoder();
        String namespace = Utils.extractNamespaceFromGraph(originalGraphPath);
        ShapesExtractor shapesExtractor = new ShapesExtractor(encoder, sts, classToEntityCount, classToPropWithObjTypes, namespace);
        shapesExtractor.generateAndSaveShapes(true, 0.5, 10);
        // Convert the RDF4J model (from ShapesExtractor) to a Jena model.
        Model extractedShapes = convertRDF4JToJena(shapesExtractor.getGeneratedShapesModel());
        if (extractedShapes.isEmpty()) {
            System.err.println("Delta shapes extraction produced an empty model. Falling back to original shapes.");
            extractedShapes = RDFDataMgr.loadModel(originalShapePath, Lang.TURTLE);
        }
        // Convert the Jena model to an RDF4J model, since ShapesExtractor.writeModelToFile expects an RDF4J model.
        org.eclipse.rdf4j.model.Model rdf4jExtractedShapes = convertJenaToRDF4J(extractedShapes);
        ShapesExtractor.writeModelToFile("Merged_SHACL.ttl", rdf4jExtractedShapes);
        Utils.logRuntime(runtimeLogPath, "Generate SHACL Shapes", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        startTime = System.nanoTime();
        startMemory = getMemoryUsage();
        ShapesMerger shapesMerger = new ShapesMerger(sts, newSts);
        org.eclipse.rdf4j.model.Model mergedShapesModel = shapesMerger.merge(originalShapePath, generateOutputFilePath("_DeltaShapes.ttl"), runtimeLogPath);
        if (mergedShapesModel.isEmpty()) {
            System.err.println("Merged shapes model is empty, falling back to original shapes.");
            mergedShapesModel = convertJenaToRDF4J(RDFDataMgr.loadModel(originalShapePath, Lang.TURTLE));
        }
        System.out.println("Merged SHACL shapes model size: " + mergedShapesModel.size());
        String mergedShapesOutputPath = generateOutputFilePath("_Merged_SHACL.ttl");
        shapesMerger.saveMergedModel(mergedShapesModel, mergedShapesOutputPath);
        Utils.logRuntime(runtimeLogPath, "Merged SHACL Shapes", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));

        startTime = System.nanoTime();
        startMemory = getMemoryUsage();
        saveDataStructures();
        Utils.logRuntime(runtimeLogPath, "Save Updated Data Structures", calculateElapsedTime(startTime), calculateMemoryUsage(startMemory));
    }

    // Helper: Convert an RDF4J Model to a Jena Model, handling blank nodes.
    private static Model convertRDF4JToJena(org.eclipse.rdf4j.model.Model rdf4jModel) {
        Model jenaModel = ModelFactory.createDefaultModel();
        org.eclipse.rdf4j.model.ValueFactory vf = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance();
        for (org.eclipse.rdf4j.model.Statement stmt : rdf4jModel) {
            Resource subj;
            if (stmt.getSubject().stringValue().startsWith("_:")) {
                subj = jenaModel.createResource();
            } else {
                subj = jenaModel.createResource(stmt.getSubject().stringValue());
            }
            org.apache.jena.rdf.model.Property pred = jenaModel.createProperty(stmt.getPredicate().stringValue());
            org.apache.jena.rdf.model.RDFNode obj;
            if (stmt.getObject() instanceof org.eclipse.rdf4j.model.Literal) {
                obj = jenaModel.createLiteral(((org.eclipse.rdf4j.model.Literal) stmt.getObject()).getLabel());
            } else {
                String objStr = stmt.getObject().stringValue();
                if (objStr.startsWith("_:")) {
                    obj = jenaModel.createResource();
                } else {
                    obj = jenaModel.createResource(objStr);
                }
            }
            jenaModel.add(subj, pred, obj);
        }
        return jenaModel;
    }

    // Helper: Convert a Jena Model to an RDF4J Model, handling blank nodes.
    private static org.eclipse.rdf4j.model.Model convertJenaToRDF4J(Model jenaModel) {
        org.eclipse.rdf4j.model.Model rdf4jModel = new LinkedHashModel();
        org.eclipse.rdf4j.model.ValueFactory vf = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance();
        jenaModel.listStatements().forEachRemaining(stmt -> {
            org.eclipse.rdf4j.model.Resource subj;
            if (stmt.getSubject().isAnon()) {
                subj = vf.createBNode(stmt.getSubject().getId().getLabelString());
            } else {
                subj = vf.createIRI(stmt.getSubject().getURI());
            }
            org.eclipse.rdf4j.model.IRI pred = vf.createIRI(stmt.getPredicate().getURI());
            org.eclipse.rdf4j.model.Value obj;
            if (stmt.getObject().isLiteral()) {
                obj = vf.createLiteral(stmt.getObject().asLiteral().getString());
            } else {
                if (stmt.getObject().isAnon()) {
                    obj = vf.createBNode(stmt.getObject().asResource().getId().getLabelString());
                } else {
                    obj = vf.createIRI(stmt.getObject().asResource().getURI());
                }
            }
            rdf4jModel.add(subj, pred, obj);
        });
        return rdf4jModel;
    }

    private static String generateOutputFilePath(String suffix) {
        return outputFilePath + datasetName + suffix;
    }

    private static void loadConfig() throws IOException {
        datasetPath = ConfigManager.getProperty("dataset_path");
        originalGraphPath = ConfigManager.getProperty("originalGraphPath");
        originalShapePath = ConfigManager.getProperty("SHAPES_FILE_PATH");
        outputFilePath = ConfigManager.getProperty("output_file_path");
        datasetName = FilesUtil.getFileName(datasetPath);
        deltaShapePath = generateOutputFilePath("_QSE_FULL_SHACL.ttl");
        graphDataPath = ConfigManager.getProperty("graph_data_path");
        if (graphDataPath == null || graphDataPath.isEmpty()) {
            graphDataPath = "/home/baya/COSEImpr/COSE/data_structure_dbpedia.kryo";
        }
        if (!graphDataPath.endsWith(".kryo")) {
            graphDataPath += ".kryo";
        }
        SAMPLE_SIZE = Integer.parseInt(ConfigManager.getProperty("SAMPLE_SIZE"));
        useFullGraphs = Boolean.parseBoolean(ConfigManager.getProperty("USE_FULL_GRAPHS"));
    }

    private static void populateSupportConfidenceForOriginalShapes(String shapePath, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts) {
        Model originalModel = RDFDataMgr.loadModel(shapePath, Lang.TURTLE);
        originalModel.listStatements().forEachRemaining(stmt -> {
            Tuple3<Integer, Integer, Integer> key = extractKey(stmt);
            sts.computeIfAbsent(key, k -> new SupportConfidence()).incrementSupport();
        });
    }

    private static Tuple3<Integer, Integer, Integer> extractKey(Statement stmt) {
        int subject = (stmt.getSubject().getURI() != null) ? stmt.getSubject().getURI().hashCode() : 0;
        int predicate = (stmt.getPredicate().getURI() != null) ? stmt.getPredicate().getURI().hashCode() : 0;
        int object;
        if (stmt.getObject().isResource()) {
            object = (stmt.getObject().asResource().getURI() != null) ? stmt.getObject().asResource().getURI().hashCode() : 0;
        } else {
            object = stmt.getObject().asLiteral().getString().hashCode();
        }
        return new Tuple3<>(subject, predicate, object);
    }

    private static Map<Integer, Map<Integer, Set<Integer>>> extractGraphData(Model graph) {
        Map<Integer, Map<Integer, Set<Integer>>> cpot = new ConcurrentHashMap<>();
        graph.listStatements().forEachRemaining(stmt -> {
            Resource subject = stmt.getSubject();
            org.apache.jena.rdf.model.Property predicate = stmt.getPredicate();
            org.apache.jena.rdf.model.RDFNode object = stmt.getObject();
            int classId = encodeResource(subject);
            int predicateId = encodeResource(predicate);
            int objectTypeId = object.isResource() ? encodeResource(object.asResource()) : encodeLiteralType(object);
            cpot.computeIfAbsent(classId, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(predicateId, k -> ConcurrentHashMap.newKeySet())
                    .add(objectTypeId);
        });
        return cpot;
    }

    private static Map<Integer, Integer> extractEntityCount(Model graph) {
        Map<Integer, Integer> entityCount = new ConcurrentHashMap<>();
        graph.listStatements(null, RDF.type, (org.apache.jena.rdf.model.RDFNode) null).forEachRemaining(stmt -> {
            Resource subject = stmt.getSubject();
            Resource classType = stmt.getObject().asResource();
            int classId = encodeResource(classType);
            entityCount.merge(classId, 1, Integer::sum);
        });
        return entityCount;
    }

    private static Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> extractSupportConfidence(Model graph) {
        Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts = new ConcurrentHashMap<>();
        graph.listStatements().forEachRemaining(stmt -> {
            Resource subject = stmt.getSubject();
            org.apache.jena.rdf.model.Property predicate = stmt.getPredicate();
            org.apache.jena.rdf.model.RDFNode object = stmt.getObject();
            int classId = encodeResource(subject);
            int predicateId = encodeResource(predicate);
            int objectTypeId = object.isResource() ? encodeResource(object.asResource()) : encodeLiteralType(object);
            Tuple3<Integer, Integer, Integer> triplet = new Tuple3<>(classId, predicateId, objectTypeId);
            sts.computeIfAbsent(triplet, k -> new SupportConfidence()).incrementSupport();
        });
        return sts;
    }

    public static int encodeResource(Resource resource) {
        if (resource == null || resource.getURI() == null) {
            throw new IllegalArgumentException("Resource URI cannot be null.");
        }
        return resourceEncoder.encode(resource.getURI());
    }

    public static int encodeLiteralType(org.apache.jena.rdf.model.RDFNode literalNode) {
        if (!literalNode.isLiteral()) {
            throw new IllegalArgumentException("Node is not a literal.");
        }
        org.apache.jena.rdf.model.Literal literal = literalNode.asLiteral();
        String datatypeURI = literal.getDatatypeURI();
        return datatypeURI != null ? resourceEncoder.encode(datatypeURI) : -1;
    }

    private static Model loadModel(String filePath, Lang lang) {
        return RDFDataMgr.loadModel(filePath, lang);
    }

    private static boolean initializeDataStructures(String originalGraphPath, String runtimeLogPath) {
        File kryoFile = new File(graphDataPath);
        if (kryoFile.exists()) {
            System.out.println("Loading data structures from " + graphDataPath + "...");
            try {
                loadDataStructures();
                return true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to load data structures: " + e.getMessage(), e);
            }
        } else {
            parseGraphStreaming(originalGraphPath, classToPropWithObjTypes, classToEntityCount, sts);
            saveDataStructures();
            return false;
        }
    }

    private static void parseGraphStreaming(String graphPath, Map<Integer, Map<Integer, Set<Integer>>> targetCpot,
                                            Map<Integer, Integer> targetCec, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> targetSts) {
        RDFParser.source(graphPath).lang(Lang.NTRIPLES).parse(new StreamRDFBase() {
            @Override
            public void triple(org.apache.jena.graph.Triple triple) {
                try {
                    Resource subject = triple.getSubject().isURI() ? model.createResource(triple.getSubject().getURI()) : model.createResource();
                    org.apache.jena.rdf.model.Property predicate = triple.getPredicate().isURI() ? model.createProperty(triple.getPredicate().getURI()) : null;
                    org.apache.jena.rdf.model.RDFNode object = triple.getObject().isURI() ? model.createResource(triple.getObject().getURI()) : model.createLiteral(triple.getObject().toString());
                    if (subject != null && predicate != null) {
                        Statement stmt = model.createStatement(subject, predicate, object);
                        model.add(stmt);
                        int classId = encodeNode(subject);
                        int predicateId = encodeNode(predicate);
                        int objectTypeId = encodeNode(object);
                        targetCpot.computeIfAbsent(classId, k -> new ConcurrentHashMap<>())
                                .computeIfAbsent(predicateId, k -> ConcurrentHashMap.newKeySet())
                                .add(objectTypeId);
                        Tuple3<Integer, Integer, Integer> triplet = new Tuple3<>(classId, predicateId, objectTypeId);
                        targetSts.computeIfAbsent(triplet, k -> new SupportConfidence()).incrementSupport();
                        targetCec.merge(classId, 1, Integer::sum);
                    }
                } catch (IllegalArgumentException e) {
                    // Skip malformed triples.
                }
            }
        });
    }

    private static int encodeNode(Resource node) {
        if (node != null && node.getURI() != null) {
            return resourceEncoder.encode(node.getURI());
        }
        return -1;
    }

    private static int encodeNode(org.apache.jena.rdf.model.RDFNode node) {
        if (node.isResource()) {
            return encodeNode(node.asResource());
        } else {
            return encodeLiteralType(node);
        }
    }

    public static int encodeLiteral(org.apache.jena.graph.Node literalNode) {
        if (!literalNode.isLiteral()) {
            throw new IllegalArgumentException("Node is not a literal: " + literalNode);
        }
        String datatypeURI = literalNode.getLiteralDatatypeURI();
        return datatypeURI != null ? resourceEncoder.encode(datatypeURI) : -1;
    }

    private static void loadDataStructures() throws IOException {
        System.out.println("Loading data structures from Kryo file...");
        try (Input input = new Input(new BufferedInputStream(new FileInputStream(graphDataPath)))) {
            kryo.register(HashMap.class);
            kryo.register(ConcurrentHashMap.class);
            kryo.register(HashSet.class);
            kryo.register(Tuple3.class);
            kryo.register(SupportConfidence.class);
            Map<Integer, Map<Integer, Set<Integer>>> serializedCpot = kryo.readObject(input, HashMap.class);
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> loadedSts = kryo.readObject(input, HashMap.class);
            Map<Integer, Integer> loadedCec = kryo.readObject(input, HashMap.class);
            System.out.println("Deserialized data structures: " + serializedCpot.size() + " " + loadedSts.size() + " " + loadedCec.size());
            serializedCpot.forEach((key, value) -> {
                Map<Integer, Set<Integer>> concurrentValue = new ConcurrentHashMap<>();
                value.forEach((k, v) -> {
                    Set<Integer> keySetView = ConcurrentHashMap.newKeySet();
                    keySetView.addAll(v);
                    concurrentValue.put(k, keySetView);
                });
                classToPropWithObjTypes.put(key, concurrentValue);
            });
            sts.putAll(loadedSts);
            classToEntityCount.putAll(loadedCec);
            System.out.println("Data structures loaded successfully.");
        } catch (IOException e) {
            System.err.println("IOException: Unable to read Kryo file. Details: " + e.getMessage());
            throw e;
        }
    }

    private static void saveDataStructures() {
        System.out.println("Saving updated data structures to " + graphDataPath + "...");
        try (Output output = new Output(new BufferedOutputStream(new FileOutputStream(graphDataPath)))) {
            kryo.register(HashMap.class);
            kryo.register(ConcurrentHashMap.class);
            kryo.register(HashSet.class);
            kryo.register(Tuple3.class);
            kryo.register(SupportConfidence.class);
            Map<Integer, Map<Integer, Set<Integer>>> serializableCpot = new HashMap<>();
            classToPropWithObjTypes.forEach((key, value) -> {
                Map<Integer, Set<Integer>> newValue = new HashMap<>();
                value.forEach((k, v) -> newValue.put(k, new HashSet<>(v)));
                serializableCpot.put(key, newValue);
            });
            kryo.writeObject(output, serializableCpot);
            kryo.writeObject(output, sts);
            kryo.writeObject(output, classToEntityCount);
            System.out.println("Data structures saved successfully.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to save data structures: " + e.getMessage(), e);
        }
    }

    private static long getMemoryUsage() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    private static long calculateElapsedTime(long startTime) {
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private static long calculateMemoryUsage(long startMemory) {
        long endMemory = getMemoryUsage();
        return (endMemory - startMemory) / (1024 * 1024);
    }
}
