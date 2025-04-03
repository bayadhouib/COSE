package cs.utils;

import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import java.util.*;

public class TSSSampler {

    private final Map<Integer, Map<Integer, Set<Integer>>> cpot;
    private final double alpha;
    private final int kmin;

    public TSSSampler(Map<Integer, Map<Integer, Set<Integer>>> cpot, double alpha, int kmin) {
        this.cpot = cpot;
        this.alpha = alpha;
        this.kmin = kmin;
    }

    // Perform Type-Specific Sampling for an entity.
    public void sample(Resource entity, Model updatedGraph, Model subgraph, Set<Resource> visitedNodes) {
        Set<Integer> classTypes = getClassTypes(entity);
        int threshold = Math.max((int)(alpha * updatedGraph.size()), kmin);

        for (Integer classType : classTypes) {
            Map<Integer, Set<Integer>> properties = cpot.get(classType);
            if (properties == null) continue;
            int sampledCount = 0;
            for (Map.Entry<Integer, Set<Integer>> entry : properties.entrySet()) {
                if (sampledCount >= threshold) break;
                Integer property = entry.getKey();
                Set<Integer> objectTypes = entry.getValue();
                for (Integer objType : objectTypes) {
                    Resource sampledEntity = createResourceFromType(objType);
                    if (!visitedNodes.contains(sampledEntity)) {
                        visitedNodes.add(sampledEntity);
                        addRelevantStatements(sampledEntity, updatedGraph, subgraph);
                        sampledCount++;
                        if (sampledCount >= threshold) break;
                    }
                }
            }
        }
    }

    // Add relevant statements from the updated graph to the subgraph for the given entity.
    public void addRelevantStatements(Resource entity, Model updatedGraph, Model subgraph) {
        StmtIterator stmtIterator = updatedGraph.listStatements(entity, null, (RDFNode) null);
        List<Statement> stmts = new ArrayList<>();
        while (stmtIterator.hasNext()) {
            stmts.add(stmtIterator.nextStatement());
        }
        synchronized (subgraph) {
            subgraph.add(stmts);
        }
    }

    // Retrieve the class types of the entity from its rdf:type properties.
    private Set<Integer> getClassTypes(Resource entity) {
        Set<Integer> classTypes = new HashSet<>();
        StmtIterator typeIter = entity.listProperties(RDF.type);
        while (typeIter.hasNext()) {
            Statement stmt = typeIter.nextStatement();
            if (stmt.getObject().isResource()) {
                int typeId = cs.Main.encodeResource(stmt.getObject().asResource());
                classTypes.add(typeId);
            }
        }
        return classTypes;
    }

    // Create a resource based on a type; uses a namespace from configuration.
    private Resource createResourceFromType(Integer type) {
        String namespace = ConfigManager.getProperty("namespace");
        if (namespace == null || namespace.isEmpty()) {
            namespace = "http://default.namespace.org/";
        }
        return ResourceFactory.createResource(namespace + "type/" + type);
    }
}
