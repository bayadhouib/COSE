package cs.cose;

import cs.Main;
import cs.cose.encoders.StringEncoder;
import cs.utils.*;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Parser {
    String rdfFilePath;
    Integer expectedNumberOfClasses;
    Integer expNoOfInstances;
    StringEncoder stringEncoder;
    StatsComputer statsComputer;
    String typePredicate;
    Model rdfModel;

    Map<Node, EntityData> entityDataHashMap;
    Map<Integer, Integer> classEntityCount;
    Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes;
    Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport;

    public Parser(Model model, int expNoOfClasses, int expNoOfInstances, String typePredicate) {
        this.rdfModel = model;
        this.expectedNumberOfClasses = expNoOfClasses;
        this.expNoOfInstances = expNoOfInstances;
        this.typePredicate = typePredicate;
        this.classEntityCount = new ConcurrentHashMap<>((int) ((expectedNumberOfClasses) / 0.75 + 1));
        this.classToPropWithObjTypes = new ConcurrentHashMap<>((int) ((expectedNumberOfClasses) / 0.75 + 1));
        this.entityDataHashMap = new ConcurrentHashMap<>((int) ((expNoOfInstances) / 0.75 + 1));
        this.stringEncoder = new StringEncoder();
    }

    public void run() {
        entityExtractionFromModel();
        entityConstraintsExtractionFromModel();
        computeSupportConfidence();
        extractSHACLShapes(true, Main.shapesFromSpecificClasses);
    }

    protected void entityExtractionFromModel() {
        StopWatch watch = new StopWatch();
        watch.start();
        rdfModel.forEach(statement -> {
            try {
                if (statement.getPredicate().stringValue().equals(typePredicate)) {
                    int objID = stringEncoder.encode(statement.getObject().stringValue());
                    Node subjectNode = new org.semanticweb.yars.nx.Resource(statement.getSubject().stringValue());
                    EntityData entityData = entityDataHashMap.get(subjectNode);
                    if (entityData == null) {
                        entityData = new EntityData();
                    }
                    entityData.getClassTypes().add(objID);
                    entityDataHashMap.put(subjectNode, entityData);
                    classEntityCount.merge(objID, 1, Integer::sum);
                }
            } catch (Exception e) {
                System.err.println("Error processing statement: " + statement + ". Error: " + e.getMessage());
            }
        });
        watch.stop();
        Utils.logTime("firstPass", TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));
    }

    protected void entityConstraintsExtractionFromModel() {
        StopWatch watch = new StopWatch();
        watch.start();
        rdfModel.forEach(statement -> {
            try {
                Set<Integer> objTypesIDs = new HashSet<>(10);
                Set<Tuple2<Integer, Integer>> prop2objTypeTuples = new HashSet<>(10);
                Node entityNode = new org.semanticweb.yars.nx.Resource(statement.getSubject().stringValue());
                String objectType = extractObjectType(statement.getObject().stringValue());
                int propID = stringEncoder.encode(statement.getPredicate().stringValue());

                if (objectType.equals("IRI")) {
                    objTypesIDs = parseIriTypeObjectFromModel(objTypesIDs, prop2objTypeTuples, statement, entityNode, propID);
                } else {
                    parseLiteralTypeObjectFromModel(objTypesIDs, entityNode, objectType, propID);
                }
                updateClassToPropWithObjTypesMap(objTypesIDs, entityNode, propID);
            } catch (Exception e) {
                System.err.println("Error processing statement: " + statement + ". Error: " + e.getMessage());
            }
        });
        watch.stop();
        Utils.logTime("secondPhase", TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));
    }

    public void computeSupportConfidence() {
        StopWatch watch = new StopWatch();
        watch.start();
        shapeTripletSupport = new ConcurrentHashMap<>((int) ((expectedNumberOfClasses) / 0.75 + 1));
        statsComputer = new StatsComputer();
        statsComputer.setShapeTripletSupport(shapeTripletSupport);
        statsComputer.computeSupportConfidence(entityDataHashMap, classEntityCount);
        watch.stop();
        Utils.logTime("computeSupportConfidence", TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));
    }

    protected void extractSHACLShapes(Boolean performPruning, Boolean qseFromSpecificClasses) {
        StopWatch watch = new StopWatch();
        watch.start();
        String methodName = "extractSHACLShapes:No Pruning";
        ShapesExtractor se = new ShapesExtractor(stringEncoder, shapeTripletSupport, classEntityCount, typePredicate);
        se.setPropWithClassesHavingMaxCountOne(statsComputer.getPropWithClassesHavingMaxCountOne());

        if (qseFromSpecificClasses)
            classToPropWithObjTypes = Utility.extractShapesForSpecificClasses(classToPropWithObjTypes, classEntityCount, stringEncoder);

        se.constructDefaultShapes(classToPropWithObjTypes);
        if (performPruning) {
            StopWatch watchForPruning = new StopWatch();
            watchForPruning.start();
            ExperimentsUtil.getSupportConfRange().forEach((conf, supportRange) -> {
                supportRange.forEach(supp -> {
                    StopWatch innerWatch = new StopWatch();
                    innerWatch.start();
                    se.constructPrunedShapes(classToPropWithObjTypes, conf, supp);
                    innerWatch.stop();
                    Utils.logTime(conf + "_" + supp + "", TimeUnit.MILLISECONDS.toSeconds(innerWatch.getTime()), TimeUnit.MILLISECONDS.toMinutes(innerWatch.getTime()));
                });
            });
            methodName = "extractSHACLShapes";
            watchForPruning.stop();
            Utils.logTime(methodName + "-Time.For.Pruning.Only", TimeUnit.MILLISECONDS.toSeconds(watchForPruning.getTime()), TimeUnit.MILLISECONDS.toMinutes(watchForPruning.getTime()));
        }

        ExperimentsUtil.prepareCsvForGroupedStackedBarChart(Constants.EXPERIMENTS_RESULT, Constants.EXPERIMENTS_RESULT_CUSTOM, true);
        watch.stop();
        Utils.logTime(methodName, TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));
    }

    private Set<Integer> parseIriTypeObjectFromModel(Set<Integer> objTypesIDs, Set<Tuple2<Integer, Integer>> prop2objTypeTuples, Statement statement, Node subject, int propID) {
        try {
            EntityData currEntityData = entityDataHashMap.get(new org.semanticweb.yars.nx.Resource(statement.getObject().stringValue()));
            if (currEntityData != null && currEntityData.getClassTypes().size() != 0) {
                objTypesIDs = currEntityData.getClassTypes();
                for (Integer node : objTypesIDs) {
                    prop2objTypeTuples.add(new Tuple2<>(propID, node));
                }
                addEntityToPropertyConstraints(prop2objTypeTuples, subject);
            } else {
                int objID = stringEncoder.encode(Constants.OBJECT_UNDEFINED_TYPE);
                objTypesIDs.add(objID);
                prop2objTypeTuples = Collections.singleton(new Tuple2<>(propID, objID));
                addEntityToPropertyConstraints(prop2objTypeTuples, subject);
            }
        } catch (Exception e) {
            System.err.println("Error processing IRI type object: " + statement.getObject() + ". Error: " + e.getMessage());
        }
        return objTypesIDs;
    }

    private void parseLiteralTypeObjectFromModel(Set<Integer> objTypes, Node subject, String objectType, int propID) {
        try {
            Set<Tuple2<Integer, Integer>> prop2objTypeTuples;
            int objID = stringEncoder.encode(objectType);
            objTypes.add(objID);
            prop2objTypeTuples = Collections.singleton(new Tuple2<>(propID, objID));
            addEntityToPropertyConstraints(prop2objTypeTuples, subject);
        } catch (Exception e) {
            System.err.println("Error processing literal type object: " + objectType + ". Error: " + e.getMessage());
        }
    }

    private void updateClassToPropWithObjTypesMap(Set<Integer> objTypesIDs, Node entityNode, int propID) {
        EntityData entityData = entityDataHashMap.get(entityNode);
        if (entityData != null) {
            for (Integer entityTypeID : entityData.getClassTypes()) {
                Map<Integer, Set<Integer>> propToObjTypes = classToPropWithObjTypes.computeIfAbsent(entityTypeID, k -> new ConcurrentHashMap<>());
                Set<Integer> classObjTypes = propToObjTypes.computeIfAbsent(propID, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
                classObjTypes.addAll(objTypesIDs);
                propToObjTypes.put(propID, classObjTypes);
                classToPropWithObjTypes.put(entityTypeID, propToObjTypes);
            }
        }
    }

    protected void addEntityToPropertyConstraints(Set<Tuple2<Integer, Integer>> prop2objTypeTuples, Node subject) {
        EntityData currentEntityData = entityDataHashMap.get(subject);
        if (currentEntityData == null) {
            currentEntityData = new EntityData();
        }
        for (Tuple2<Integer, Integer> tuple2 : prop2objTypeTuples) {
            currentEntityData.addPropertyConstraint(tuple2._1, tuple2._2);
            if (Main.extractMaxCardConstraints) {
                currentEntityData.addPropertyCardinality(tuple2._1);
            }
        }
        entityDataHashMap.put(subject, currentEntityData);
    }

    protected String extractObjectType(String literalIri) {
        try {
            Literal theLiteral = new Literal(literalIri, true);
            String type = null;
            if (theLiteral.getDatatype() != null) {
                type = theLiteral.getDatatype().toString();
            } else if (theLiteral.getLanguageTag() != null) {
                type = "<" + RDF.LANGSTRING + ">";
            } else {
                if (Utils.isValidIRI(literalIri)) {
                    if (SimpleValueFactory.getInstance().createIRI(literalIri).isIRI()) type = "IRI";
                } else {
                    type = "<" + XSD.STRING + ">";
                }
            }
            return type;
        } catch (Exception e) {
            System.err.println("Error extracting object type for: " + literalIri + ". Error: " + e.getMessage());
            return "<" + XSD.STRING + ">";
        }
    }
}
