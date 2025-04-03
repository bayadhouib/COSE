package cs.utils;

import cs.Main;
import cs.utils.Tuple2;
import cs.utils.Tuple3;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;

import java.util.*;


public class StatsComputer {
    private final Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport;
    private final Map<Integer, Set<Integer>> propWithClassesHavingMaxCountOne; // Size O(P*T)

    public StatsComputer() {
        this.shapeTripletSupport = new HashMap<>();
        this.propWithClassesHavingMaxCountOne = new HashMap<>();
    }


    public void computeSupportConfidenceFromDeltaGraph(Model deltaGraph, Map<Integer, Integer> classToEntityCount) {
        StmtIterator stmtIterator = deltaGraph.listStatements();

        while (stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();

            Resource subject = stmt.getSubject();
            Property predicate = stmt.getPredicate();
            RDFNode object = stmt.getObject();

            // Retrieve subject types (rdf:type)
            StmtIterator typeIterator = deltaGraph.listStatements(subject, RDF.type, (RDFNode) null);
            while (typeIterator.hasNext()) {
                Statement typeStmt = typeIterator.nextStatement();
                Resource classType = typeStmt.getObject().asResource();

                // Encode the class, predicate, and object type
                int classId = Main.encodeResource(classType);
                int predicateId = Main.encodeResource(predicate);
                int objectTypeId = object.isResource() ? Main.encodeResource(object.asResource()) : Main.encodeLiteralType(object);

                // Create the type-property-object tuple
                Tuple3<Integer, Integer, Integer> tuple3 = new Tuple3<>(classId, predicateId, objectTypeId);

                // Update support in sts
                shapeTripletSupport.computeIfAbsent(tuple3, k -> new SupportConfidence()).incrementSupport();

                // Track properties with max count = 1 (if enabled)
                if (Main.extractMaxCardConstraints) {
                    propWithClassesHavingMaxCountOne.putIfAbsent(predicateId, new HashSet<>());
                    propWithClassesHavingMaxCountOne.get(predicateId).add(classId);
                }
            }
        }

        // Compute confidence for each triplet in sts
        for (Map.Entry<Tuple3<Integer, Integer, Integer>, SupportConfidence> entry : shapeTripletSupport.entrySet()) {
            Tuple3<Integer, Integer, Integer> tuple3 = entry.getKey();
            SupportConfidence supportConfidence = entry.getValue();

            int classId = tuple3._1();
            int totalEntityCount = classToEntityCount.getOrDefault(classId, 0);

            if (totalEntityCount > 0) {
                double confidence = (double) supportConfidence.getSupport() / totalEntityCount;
                supportConfidence.setConfidence(confidence);
            } else {
                supportConfidence.setConfidence(0.0);
            }
        }

        System.out.println("Support and confidence computation completed.");
    }

    // Getters
    public Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> getShapeTripletSupport() {
        return shapeTripletSupport;
    }

    public Map<Integer, Set<Integer>> getPropWithClassesHavingMaxCountOne() {
        return propWithClassesHavingMaxCountOne;
    }
}
