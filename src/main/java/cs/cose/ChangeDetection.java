package cs.cose;

import cs.utils.SupportConfidence;
import cs.utils.Tuple3;
import cs.utils.Utils;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class ChangeDetection {

    private static final int CACHE_SIZE = 1000;

    private final Map<String, Property> propertyCache = new LinkedHashMap<String, Property>() {
        protected boolean removeEldestEntry(Map.Entry<String, Property> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    private final Map<String, Literal> literalCache = new LinkedHashMap<String, Literal>() {
        protected boolean removeEldestEntry(Map.Entry<String, Literal> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    private String datasetNamespace;

    // Direct comparison method: compares old and new data structures and writes differences as N-Triples.
    public static void detectChangesToFile(
            Map<Integer, Map<Integer, Set<Integer>>> oldCpot,
            Map<Integer, Integer> oldCec,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> oldSts,
            Map<Integer, Map<Integer, Set<Integer>>> newCpot,
            Map<Integer, Integer> newCec,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts,
            String datasetPath,
            String outputFilePath
    ) throws IOException {
        ChangeDetection changeDetection = new ChangeDetection();
        changeDetection.datasetNamespace = Utils.extractNamespaceFromGraph(datasetPath);
        // Delete output file if exists
        new File(outputFilePath).delete();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath, true))) {
            changeDetection.compareCpotAndWriteToDisk(oldCpot, newCpot, writer);
            changeDetection.compareCecAndWriteToDisk(oldCec, newCec, writer);
            changeDetection.compareStsAndWriteToDisk(oldSts, newSts, writer);
        }
    }

    private void compareCpotAndWriteToDisk(
            Map<Integer, Map<Integer, Set<Integer>>> oldCpot,
            Map<Integer, Map<Integer, Set<Integer>>> newCpot,
            BufferedWriter writer
    ) throws IOException {
        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> newClassEntry : newCpot.entrySet()) {
            int classId = newClassEntry.getKey();
            Map<Integer, Set<Integer>> newProps = newClassEntry.getValue();
            Map<Integer, Set<Integer>> oldProps = oldCpot.getOrDefault(classId, new HashMap<>());
            for (Map.Entry<Integer, Set<Integer>> newPropEntry : newProps.entrySet()) {
                int predicateId = newPropEntry.getKey();
                Set<Integer> newObjectTypes = newPropEntry.getValue();
                Set<Integer> oldObjectTypes = oldProps.getOrDefault(predicateId, new HashSet<>());
                for (Integer newObjectType : newObjectTypes) {
                    if (!oldObjectTypes.contains(newObjectType)) {
                        writer.write(String.format(
                                "<%sclass/%d> <%spredicate/%d> <%sobject/%d> .%n",
                                datasetNamespace, classId, datasetNamespace, predicateId, datasetNamespace, newObjectType
                        ));
                    }
                }
            }
        }
    }

    private void compareCecAndWriteToDisk(
            Map<Integer, Integer> oldCec,
            Map<Integer, Integer> newCec,
            BufferedWriter writer
    ) throws IOException {
        for (Map.Entry<Integer, Integer> newEntry : newCec.entrySet()) {
            int classId = newEntry.getKey();
            int newCount = newEntry.getValue();
            int oldCount = oldCec.getOrDefault(classId, 0);
            if (newCount != oldCount) {
                writer.write(String.format(
                        "<%sclass/%d> <%spredicate/entityCount> \"%d\" .%n",
                        datasetNamespace, classId, datasetNamespace, newCount
                ));
            }
        }
    }

    private void compareStsAndWriteToDisk(
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> oldSts,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts,
            BufferedWriter writer
    ) throws IOException {
        for (Map.Entry<Tuple3<Integer, Integer, Integer>, SupportConfidence> newEntry : newSts.entrySet()) {
            Tuple3<Integer, Integer, Integer> triplet = newEntry.getKey();
            SupportConfidence newSc = newEntry.getValue();
            SupportConfidence oldSc = oldSts.getOrDefault(triplet, new SupportConfidence(0, 0.0));
            if (newSc.getSupport() != oldSc.getSupport() || newSc.getConfidence() != oldSc.getConfidence()) {
                writer.write(String.format(
                        "<%sclass/%d> <%spredicate/%d> <%sobject/%d> .%n",
                        datasetNamespace, triplet._1(), datasetNamespace, triplet._2(), datasetNamespace, triplet._3()
                ));
            }
        }
    }

    // SPARQL-based change detection. Queries the endpoint for additions and modifications.
    public void detectChangesWithSparql(String sparqlEndpoint, String originalGraph, String updatedGraph, String outputFilePath) throws IOException {
        String additionsQuery = String.format(
                "SELECT ?subject ?predicate ?object WHERE { GRAPH <%s> { ?subject ?predicate ?object } " +
                        "FILTER NOT EXISTS { GRAPH <%s> { ?subject ?predicate ?object } } }", updatedGraph, originalGraph);
        String modificationsQuery = String.format(
                "SELECT ?subject ?predicate ?oldValue ?newValue WHERE { GRAPH <%s> { ?subject ?predicate ?oldValue } " +
                        "GRAPH <%s> { ?subject ?predicate ?newValue } FILTER (?oldValue != ?newValue) }", originalGraph, updatedGraph);
        HTTPRepository repo = new HTTPRepository(sparqlEndpoint);
        repo.initialize();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            TupleQuery additionsTupleQuery = repo.getConnection().prepareTupleQuery(additionsQuery);
            try (TupleQueryResult result = additionsTupleQuery.evaluate()) {
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    writer.write(String.format(
                            "<%s> <%s> <%s> .%n",
                            bindingSet.getValue("subject").stringValue(),
                            bindingSet.getValue("predicate").stringValue(),
                            bindingSet.getValue("object").stringValue()));
                }
            }
            TupleQuery modificationsTupleQuery = repo.getConnection().prepareTupleQuery(modificationsQuery);
            try (TupleQueryResult result = modificationsTupleQuery.evaluate()) {
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    writer.write(String.format(
                            "<%s> <%s> <%s> .%n",
                            bindingSet.getValue("subject").stringValue(),
                            bindingSet.getValue("predicate").stringValue(),
                            bindingSet.getValue("newValue").stringValue()));
                }
            }
        } finally {
            repo.shutDown();
        }
    }

    // Wikidata edit log based change detection. Uses the Wikidata API to retrieve changes.
    public List<Change> detectChangesFromEditHistory(String datasetId) {
        List<Change> changes = new ArrayList<>();
        try {
            String apiUrl = String.format("https://www.wikidata.org/w/api.php?action=wbgetentities&ids=%s&format=json", datasetId);
            HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() == 200) {
                try (InputStreamReader reader = new InputStreamReader(conn.getInputStream())) {
                    com.google.gson.JsonObject jsonObject = com.google.gson.JsonParser.parseReader(reader).getAsJsonObject();
                    com.google.gson.JsonObject entities = jsonObject.getAsJsonObject("entities");
                    for (String entityId : entities.keySet()) {
                        com.google.gson.JsonObject entityData = entities.getAsJsonObject(entityId);
                        com.google.gson.JsonObject claims = entityData.getAsJsonObject("claims");
                        for (String property : claims.keySet()) {
                            com.google.gson.JsonArray propertyArray = claims.getAsJsonArray(property);
                            for (int i = 0; i < propertyArray.size(); i++) {
                                com.google.gson.JsonObject claim = propertyArray.get(i).getAsJsonObject();
                                String newValue = claim.getAsJsonObject("mainsnak").toString();
                                changes.add(new Change(
                                        Change.ChangeType.ADDITION,
                                        ResourceFactory.createResource("http://www.wikidata.org/entity/" + entityId),
                                        ResourceFactory.createProperty("http://www.wikidata.org/prop/" + property),
                                        null,
                                        ResourceFactory.createPlainLiteral(newValue)
                                ));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return changes;
    }

    // Change inner class representing a detected change.
    public static class Change {
        public enum ChangeType { ADDITION, MODIFICATION }
        public ChangeType type;
        public Resource entity;
        public Property property;
        public RDFNode oldValue;
        public RDFNode newValue;
        public Change(ChangeType type, Resource entity, Property property, RDFNode oldValue, RDFNode newValue) {
            this.type = type;
            this.entity = entity;
            this.property = property;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
        @Override
        public String toString() {
            return "Change{" +
                    "type=" + type +
                    ", entity=" + entity +
                    ", property=" + property +
                    ", oldValue=" + oldValue +
                    ", newValue=" + newValue +
                    '}';
        }
    }
}
