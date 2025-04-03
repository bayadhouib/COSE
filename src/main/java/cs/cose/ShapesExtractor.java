package cs.cose;

import cs.Main;
import cs.cose.encoders.Encoder;
import cs.utils.*;
import org.apache.jena.rdf.model.RDFNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class ShapesExtractor {

    private final Encoder encoder;
    private final Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport;
    private final Map<Integer, Integer> classInstanceCount;
    private final Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes;
    private final SimpleValueFactory valueFactory;
    private final ModelBuilder builder;
    private final String namespace;

    public ShapesExtractor(
            Encoder encoder,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport,
            Map<Integer, Integer> classInstanceCount,
            Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes,
            String datasetNamespace) {

        if (encoder == null) {
            throw new IllegalArgumentException("Encoder cannot be null");
        }
        this.encoder = encoder;
        this.shapeTripletSupport = shapeTripletSupport;
        this.classInstanceCount = classInstanceCount;
        this.classToPropWithObjTypes = classToPropWithObjTypes;
        this.valueFactory = SimpleValueFactory.getInstance();
        this.namespace = datasetNamespace.endsWith("/") || datasetNamespace.endsWith("#")
                ? datasetNamespace
                : datasetNamespace + "/";
        this.builder = new ModelBuilder()
                .setNamespace("sh", SHACL.NAMESPACE)
                .setNamespace("xsd", XSD.NAMESPACE)
                .setNamespace("ns", this.namespace);
    }

    // Generates shapes, either using pruned (filtered) properties or default ones.
    public void generateAndSaveShapes(boolean usePrunedShapes, double confidenceThreshold, int supportThreshold) {
        if (usePrunedShapes) {
            constructPrunedShapes(classToPropWithObjTypes, confidenceThreshold, supportThreshold);
        } else {
            constructDefaultShapes(classToPropWithObjTypes);
        }
    }

    public void constructDefaultShapes(Map<Integer, Map<Integer, Set<Integer>>> subsetClassToPropWithObjTypes) {
        SailRepository repository = new SailRepository(new MemoryStore());
        repository.init();

        try (RepositoryConnection conn = repository.getConnection()) {
            buildAndStoreShapes(conn, subsetClassToPropWithObjTypes, false, 0.0, 0);
            processPostConstraints(conn);

            org.eclipse.rdf4j.model.Model model = new LinkedHashModel();
            RepositoryResult<Statement> stmts = conn.getStatements(null, null, null);
            while (stmts.hasNext()) {
                model.add(stmts.next());
            }
        } finally {
            repository.shutDown();
        }
    }

    public void constructPrunedShapes(Map<Integer, Map<Integer, Set<Integer>>> subsetClassToPropWithObjTypes, double confidenceThreshold, int supportThreshold) {
        File dbDir = prepareRepositoryDirectory("shapes_pruned");
        SailRepository repository = new SailRepository(new org.eclipse.rdf4j.sail.nativerdf.NativeStore(dbDir));
        repository.init();

        try (RepositoryConnection conn = repository.getConnection()) {
            buildAndStoreShapes(conn, subsetClassToPropWithObjTypes, true, confidenceThreshold, supportThreshold);
            processPostConstraints(conn);

            org.eclipse.rdf4j.model.Model model = new LinkedHashModel();
            RepositoryResult<Statement> stmts = conn.getStatements(null, null, null);
            while (stmts.hasNext()) {
                model.add(stmts.next());
            }
        } finally {
            repository.shutDown();
            deleteDirectory(dbDir);
        }
    }

    public org.eclipse.rdf4j.model.Model getGeneratedShapesModel() {
        return builder.build();
    }

    private void buildAndStoreShapes(RepositoryConnection conn, Map<Integer, Map<Integer, Set<Integer>>> dataSubset, boolean isPruned, double confidenceThreshold, int supportThreshold) {
        dataSubset.forEach((classType, propWithObjTypes) -> {
            Map<Integer, Set<Integer>> properties = isPruned
                    ? pruneProperties(classType, propWithObjTypes, confidenceThreshold, supportThreshold)
                    : propWithObjTypes;
            buildShapesForClass(classType, properties);
        });
        org.eclipse.rdf4j.model.Model model = builder.build();
        conn.add(model);
    }

    private Map<Integer, Set<Integer>> pruneProperties(Integer classType, Map<Integer, Set<Integer>> propWithObjTypes, double confidenceThreshold, int supportThreshold) {
        Map<Integer, Set<Integer>> prunedProps = new HashMap<>();
        propWithObjTypes.forEach((property, objectTypes) -> {
            Set<Integer> prunedObjectTypes = new HashSet<>();
            for (Integer objectType : objectTypes) {
                Tuple3<Integer, Integer, Integer> triplet = new Tuple3<>(classType, property, objectType);
                SupportConfidence sc = shapeTripletSupport.get(triplet);
                if (sc != null && sc.getConfidence() >= confidenceThreshold && sc.getSupport() >= supportThreshold) {
                    prunedObjectTypes.add(objectType);
                }
            }
            if (!prunedObjectTypes.isEmpty()) {
                prunedProps.put(property, prunedObjectTypes);
            }
        });
        return prunedProps;
    }

    private void buildShapesForClass(Integer classType, Map<Integer, Set<Integer>> propWithObjTypes) {
        String classIRI = encoder.decode(classType);
        if (Utils.isValidIRI(classIRI)) {
            IRI classIRIValue = valueFactory.createIRI(classIRI);
            String nodeShapeIRI = namespace + classIRIValue.getLocalName() + "Shape";

            builder.subject(nodeShapeIRI)
                    .add(RDF.TYPE, SHACL.NODE_SHAPE)
                    .add(SHACL.TARGET_CLASS, classIRIValue);

            if (classInstanceCount.containsKey(classType)) {
                builder.subject(nodeShapeIRI).add(namespace + "support", classInstanceCount.get(classType));
            }

            propWithObjTypes.forEach((property, objectTypes) -> {
                constructPropertyShape(nodeShapeIRI, property, objectTypes, classType);
            });
        }
    }

    // Constructs a property shape and infers cardinality.
    private void constructPropertyShape(String nodeShapeIRI, Integer property, Set<Integer> objectTypes, Integer classType) {
        IRI propertyIRI = valueFactory.createIRI(encoder.decode(property));
        String propertyShapeIRI = namespace + propertyIRI.getLocalName() + "PropertyShape";

        builder.subject(nodeShapeIRI)
                .add(SHACL.PROPERTY, propertyShapeIRI);

        builder.subject(propertyShapeIRI)
                .add(RDF.TYPE, SHACL.PROPERTY_SHAPE)
                .add(SHACL.PATH, propertyIRI);

        if (objectTypes.size() == 1) {
            Integer objectType = objectTypes.iterator().next();
            addSingleObjectTypeConstraints(propertyShapeIRI, property, objectType, classType);
        } else if (objectTypes.size() > 1) {
            addMultipleObjectTypeConstraints(propertyShapeIRI, property, objectTypes, classType);
        }
        // Cardinality inference: if confidence is 1.0 then minCount = 1, else 0.
        Tuple3<Integer, Integer, Integer> triplet = new Tuple3<>(classType, property, objectTypes.iterator().next());
        SupportConfidence sc = shapeTripletSupport.getOrDefault(triplet, new SupportConfidence(0, 0.0));
        int minCount = sc.getConfidence() == 1.0 ? 1 : 0;
        builder.subject(propertyShapeIRI).add(SHACL.MIN_COUNT, minCount);
    }

    private void addSingleObjectTypeConstraints(String propertyShapeIRI, Integer property, Integer objectType, Integer classType) {
        Tuple3<Integer, Integer, Integer> triplet = new Tuple3<>(classType, property, objectType);
        SupportConfidence sc = shapeTripletSupport.get(triplet);
        if (sc != null) {
            builder.subject(propertyShapeIRI)
                    .add(namespace + "support", sc.getSupport())
                    .add(namespace + "confidence", sc.getConfidence());

            String objectTypeIRI = encoder.decode(objectType);
            if (Utils.isValidIRI(objectTypeIRI)) {
                builder.subject(propertyShapeIRI).add(SHACL.CLASS, valueFactory.createIRI(objectTypeIRI));
            } else {
                builder.subject(propertyShapeIRI).add(SHACL.NODE_KIND, SHACL.LITERAL);
            }
        }
    }

    private void addMultipleObjectTypeConstraints(String propertyShapeIRI, Integer property, Set<Integer> objectTypes, Integer classType) {
        List<Resource> orList = new ArrayList<>();
        for (Integer objectType : objectTypes) {
            Tuple3<Integer, Integer, Integer> triplet = new Tuple3<>(classType, property, objectType);
            if (shapeTripletSupport.containsKey(triplet)) {
                String objectTypeIRI = encoder.decode(objectType);
                Resource orConstraint = valueFactory.createBNode();
                orList.add(orConstraint);
                if (Utils.isValidIRI(objectTypeIRI)) {
                    builder.subject(orConstraint).add(SHACL.CLASS, valueFactory.createIRI(objectTypeIRI));
                } else {
                    builder.subject(orConstraint).add(SHACL.NODE_KIND, SHACL.LITERAL);
                }
            }
        }
        if (!orList.isEmpty()) {
            Resource orNode = (Resource) RDFCollections.asRDF(orList, valueFactory.createBNode(), builder.build());
            builder.subject(propertyShapeIRI).add(SHACL.OR, orNode);
        }
    }

    private void processPostConstraints(RepositoryConnection conn) {
        PostConstraintsAnnotator annotator = new PostConstraintsAnnotator(conn);
        annotator.addShNodeConstraint();
    }

    private File prepareRepositoryDirectory(String dirName) {
        String uniqueDirName = Main.outputFilePath + dirName + "_" + System.currentTimeMillis();
        File dbDir = new File(uniqueDirName);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        return dbDir;
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    public static String writeModelToFile(String fileIdentifier, org.eclipse.rdf4j.model.Model model) {
        String path = Main.outputFilePath + Main.datasetName + "_Merged_SHACL.ttl";
        try (FileOutputStream fos = new FileOutputStream(path)) {
            Rio.write(model, fos, RDFFormat.TURTLE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return path;
    }
}
