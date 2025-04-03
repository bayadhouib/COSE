package cs.cose;

import cs.Main;
import cs.cose.encoders.ConcurrentStringEncoder;
import cs.cose.encoders.StringEncoder;
import cs.utils.*;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.semanticweb.yars.nx.Literal;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static cs.Main.datasetName;
import static cs.Main.outputFilePath;

public class Utility {


    // Extracts the literal object type.
    public static String extractObjectType(String literalIri) {
        Literal theLiteral = new Literal(literalIri, true);
        if (theLiteral.getDatatype() != null) {
            return theLiteral.getDatatype().toString(); // Literal type
        } else if (theLiteral.getLanguageTag() != null) {
            return "<" + RDF.LANGSTRING + ">"; // RDF lang type
        } else if (Utils.isValidIRI(literalIri)) {
            return SimpleValueFactory.getInstance().createIRI(literalIri).isIRI() ? "IRI" : null;
        } else {
            return "<" + XSD.STRING + ">";
        }
    }

    // Builds a SPARQL query for a specific entity and its types.
    public static String buildQuery(String entity, Set<String> types, String typePredicate) {
        StringBuilder query = new StringBuilder("PREFIX onto: <http://www.ontotext.com/> \nSELECT * FROM onto:explicit WHERE { \n");
        for (String type : types) {
            query.append("<").append(entity).append("> ").append(typePredicate).append(" <").append(type).append("> .\n");
        }
        query.append("<").append(entity).append("> ?p ?o . \n}\n");
        return query.toString();
    }

    // Builds a batch SPARQL query for multiple entities and their types.
    public static String buildBatchQuery(Set<String> types, List<String> entities, String typePredicate) {
        StringBuilder query = new StringBuilder("PREFIX onto: <http://www.ontotext.com/> \nSELECT * FROM onto:explicit WHERE { \n");
        for (String type : types) {
            query.append("?entity ").append(typePredicate).append(" <").append(type).append("> .\n");
        }
        query.append("?entity ?p ?o . \nVALUES (?entity) { \n");
        for (String entity : entities) {
            query.append("\t( <").append(entity).append("> ) \n");
        }
        query.append("} \n}\n");
        return query.toString();
    }

    // Builds a SPARQL query using placeholders for class, property, and object type.
    private static String buildQuery(String classIri, String property, String objectType, String queryFile, String typePredicate) {
        String query = FilesUtil.readQuery(queryFile)
                .replace(":Class", "<" + classIri + ">")
                .replace(":Prop", "<" + property + ">")
                .replace(":ObjectType", objectType)
                .replace(":instantiationProperty", typePredicate);
        return query;
    }


    private static String generateOutputFilePath(String suffix) {
        return outputFilePath + datasetName + suffix;
    }


    public static void writeSupportToFile(StringEncoder stringEncoder, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport, Map<Integer, List<Integer>> sampledEntitiesPerClass) {
        writeSupportToFileInternal(stringEncoder, shapeTripletSupport, sampledEntitiesPerClass);
    }


    public static void writeSupportToFile(ConcurrentStringEncoder encoder, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport, Map<Integer, List<Integer>> sampledEntitiesPerClass) {
        writeSupportToFileInternal(encoder, shapeTripletSupport, sampledEntitiesPerClass);
    }

    private static void writeSupportToFileInternal(Object encoder, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport, Map<Integer, List<Integer>> sampledEntitiesPerClass) {
        System.out.println("Started writeSupportToFile()");
        StopWatch watch = new StopWatch();
        watch.start();
        try (FileWriter fileWriter = new FileWriter(new File(Constants.TEMP_DATASET_FILE), false);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {

            for (Map.Entry<Tuple3<Integer, Integer, Integer>, SupportConfidence> entry : shapeTripletSupport.entrySet()) {
                Tuple3<Integer, Integer, Integer> tuple = entry.getKey();
                Integer count = entry.getValue().getSupport();
                String log = (encoder instanceof StringEncoder ? ((StringEncoder) encoder).decode(tuple._1) : ((ConcurrentStringEncoder) encoder).decode(tuple._1)) +
                        "|" + (encoder instanceof StringEncoder ? ((StringEncoder) encoder).decode(tuple._2) : ((ConcurrentStringEncoder) encoder).decode(tuple._2)) +
                        "|" + (encoder instanceof StringEncoder ? ((StringEncoder) encoder).decode(tuple._3) : ((ConcurrentStringEncoder) encoder).decode(tuple._3)) +
                        "|" + count + "|" + sampledEntitiesPerClass.get(tuple._1).size();
                printWriter.println(log);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        watch.stop();
        Utils.logTime("writeSupportToFile()", TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));
    }

   // Filters shapes for specific classes.
    public static Map<Integer, Map<Integer, Set<Integer>>> extractShapesForSpecificClasses(Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes, Map<Integer, Integer> classEntityCount, StringEncoder stringEncoder) {
        Map<Integer, Map<Integer, Set<Integer>>> filtered = new HashMap<>();
        String fileAddress = ConfigManager.getProperty("config_dir_path") + "pruning/classes.txt";
        List<String> classes = FilesUtil.readAllLinesFromFile(fileAddress);
        classes.forEach(classIri -> {
            int key = stringEncoder.encode(classIri);
            Map<Integer, Set<Integer>> value = classToPropWithObjTypes.get(key);
            if (classEntityCount.containsKey(key)) {
                filtered.put(key, value);
            }
        });
        return filtered;
    }


    public static List<String> getListOfClasses() {
        String fileAddress = ConfigManager.getProperty("config_dir_path") + "pruning/classes.txt";
        return FilesUtil.readAllLinesFromFile(fileAddress);
    }


    public static void writeClassFrequencyInFile(Map<Integer, Integer> classEntityCount, StringEncoder stringEncoder) {
        String fileNameAndPath = outputFilePath + "/classFrequency.csv";
        try (FileWriter fileWriter = new FileWriter(fileNameAndPath, false);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {
            printWriter.println("Class,Frequency");
            classEntityCount.forEach((classVal, entityCount) -> printWriter.println(stringEncoder.decode(classVal) + "," + entityCount));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static RepositoryConnection readFileAsRdf4JModel(String inputFileAddress) {
        try (InputStream input = new FileInputStream(inputFileAddress)) {
            Model model = Rio.parse(input, "", RDFFormat.TURTLE);
            Repository db = new SailRepository(new MemoryStore());
            RepositoryConnection conn = db.getConnection();
            conn.add(model);
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
