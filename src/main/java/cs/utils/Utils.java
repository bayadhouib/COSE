package cs.utils;

import cs.Main;
import cs.cose.ChangeDetection;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Utils {
    private static long secondsTotal;
    private static long minutesTotal;

    public static void getCurrentTimeStamp() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
        dtf.format(now);
    }

    public static boolean isValidIRI(String iri) {
        if (iri == null) {
            return false;
        }
        return iri.indexOf(':') > 0;
    }

    public static void logRuntime(String runtimeLogPath, String phase, long elapsedTimeMs, long memoryUsageMb) {
        try (FileWriter writer = new FileWriter(runtimeLogPath, true)) {
            writer.write(phase + " | Time: " + elapsedTimeMs + "ms | Memory: " + memoryUsageMb + "MB\n");
            System.out.println(phase + " completed: " + elapsedTimeMs + "ms | Memory: " + memoryUsageMb + "MB");
        } catch (IOException e) {
            System.err.println("Error writing runtime log: " + e.getMessage());
        }
    }


    public static Model convertRdf4jToJena(org.eclipse.rdf4j.model.Model rdf4jModel) {
        Model jenaModel = ModelFactory.createDefaultModel();

        for (org.eclipse.rdf4j.model.Statement stmt : rdf4jModel) {
            Resource subject = convertRdf4jToJena(stmt.getSubject());
            Property predicate = convertRdf4jToJena(stmt.getPredicate());
            RDFNode object = convertRdf4jToJena(stmt.getObject());
            jenaModel.add(subject, predicate, object);
        }

        return jenaModel;
    }

    private static Resource convertRdf4jToJena(org.eclipse.rdf4j.model.Resource resource) {
        if (resource instanceof org.eclipse.rdf4j.model.IRI) {
            return ResourceFactory.createResource(resource.stringValue());
        } else if (resource instanceof org.eclipse.rdf4j.model.BNode) {
            return ResourceFactory.createResource(((org.eclipse.rdf4j.model.BNode) resource).getID());
        }
        return null;
    }

    private static Property convertRdf4jToJena(org.eclipse.rdf4j.model.IRI predicate) {
        return ResourceFactory.createProperty(predicate.stringValue());
    }

    public static void loadStats(String filePath, Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes,
                                 Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            @SuppressWarnings("unchecked")
            Map<Integer, Map<Integer, Set<Integer>>> loadedClassToProp = (Map<Integer, Map<Integer, Set<Integer>>>) ois.readObject();
            @SuppressWarnings("unchecked")
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> loadedSTS = (Map<Tuple3<Integer, Integer, Integer>, SupportConfidence>) ois.readObject();

            classToPropWithObjTypes.clear();
            sts.clear();

            classToPropWithObjTypes.putAll(loadedClassToProp);
            sts.putAll(loadedSTS);

            System.out.println("Stats successfully loaded from: " + filePath);
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error loading stats from file: " + filePath);
            e.printStackTrace();
        }
    }

    public static <T> List<List<T>> partitionList(List<T> list, int partitionSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += partitionSize) {
            partitions.add(list.subList(i, Math.min(i + partitionSize, list.size())));
        }
        return partitions;
    }


    public static List<ChangeDetection.Change> readChangesFromFile(String changesFilePath) throws IOException {
        List<ChangeDetection.Change> changes = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(changesFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split(", ");
                if (parts.length < 3) {
                    System.err.println("Skipping malformed line: " + line);
                    continue;
                }

                try {
                    String changeTypeStr = null, className = null, predicate = null, objectType = null;
                    int support = 0;
                    double confidence = 0.0;

                    for (String part : parts) {
                        String[] keyValue = part.split("=");
                        if (keyValue.length != 2) {
                            System.err.println("Skipping malformed segment: " + part);
                            continue;
                        }
                        String key = keyValue[0].trim();
                        String value = keyValue[1].trim();

                        switch (key) {
                            case "ChangeType":
                                changeTypeStr = value;
                                break;
                            case "Class":
                                className = value;
                                break;
                            case "Predicate":
                                predicate = value;
                                break;
                            case "ObjectType":
                                objectType = value;
                                break;
                            case "Support":
                                support = Integer.parseInt(value);
                                break;
                            case "Confidence":
                                confidence = Double.parseDouble(value);
                                break;
                            default:
                                System.err.println("Unknown key in line: " + key);
                        }
                    }

                    if (changeTypeStr == null || className == null || predicate == null || objectType == null) {
                        System.err.println("Skipping malformed line (missing required fields): " + line);
                        continue;
                    }

                    ChangeDetection.Change.ChangeType changeType =
                            "ADDITION".equalsIgnoreCase(changeTypeStr) ? ChangeDetection.Change.ChangeType.ADDITION
                                    : ChangeDetection.Change.ChangeType.MODIFICATION;

                    ChangeDetection.Change change = new ChangeDetection.Change(
                            changeType,
                            ResourceFactory.createResource(className),
                            ResourceFactory.createProperty(predicate),
                            null,
                            ResourceFactory.createPlainLiteral(objectType)
                    );

                    changes.add(change);
                } catch (Exception e) {
                    System.err.println("Error parsing line: " + line + ". Reason: " + e.getMessage());
                }
            }
        }

        return changes;
    }



    public static void saveModelToFile(Model model, String filePath, Lang lang) {
        try (OutputStream out = new FileOutputStream(filePath)) {
            RDFDataMgr.write(out, model, lang);
            System.out.println("Model saved to: " + filePath);
        } catch (IOException e) {
            System.err.println("Error saving model to file: " + e.getMessage());
        }
    }



    public static void saveStats(String filePath, Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes,
                                 Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(classToPropWithObjTypes);
            oos.writeObject(sts);

            System.out.println("Stats successfully saved to: " + filePath);
        } catch (IOException e) {
            System.err.println("Error saving stats to file: " + filePath);
            e.printStackTrace();
        }
    }



    private static RDFNode convertRdf4jToJena(org.eclipse.rdf4j.model.Value value) {
        if (value instanceof org.eclipse.rdf4j.model.Literal) {
            org.eclipse.rdf4j.model.Literal literal = (org.eclipse.rdf4j.model.Literal) value;
            return ResourceFactory.createTypedLiteral(literal.getLabel());
        } else if (value instanceof org.eclipse.rdf4j.model.Resource) {
            return convertRdf4jToJena((org.eclipse.rdf4j.model.Resource) value);
        }
        return ResourceFactory.createPlainLiteral(value.stringValue());
    }



    public static void log(String log) {
        try {
            FileWriter fileWriter = new FileWriter(Constants.RUNTIME_LOGS, true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println(log);
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String extractNamespaceFromGraph(String graphFilePath) {
        try {
            Model model = RDFDataMgr.loadModel(graphFilePath);
            StmtIterator stmtIter = model.listStatements();

            while (stmtIter.hasNext()) {
                Statement stmt = stmtIter.nextStatement();
                String subjectNamespace = getNamespace(stmt.getSubject().toString());
                String predicateNamespace = getNamespace(stmt.getPredicate().toString());
                if (isValidNamespace(subjectNamespace)) {
                    return subjectNamespace;
                }
                if (isValidNamespace(predicateNamespace)) {
                    return predicateNamespace;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "http://example.org/";
    }

    private static String getNamespace(String iri) {
        int lastSlashIndex = iri.lastIndexOf('/');
        int lastHashIndex = iri.lastIndexOf('#');
        int index = Math.max(lastSlashIndex, lastHashIndex);

        return (index > -1) ? iri.substring(0, index + 1) : null;
    }

    private static boolean isValidNamespace(String namespace) {
        return namespace != null && (namespace.startsWith("http://") || namespace.startsWith("https://"));
    }


    public static void logTime(String method, long seconds, long minutes) {
        secondsTotal += seconds;
        minutesTotal += minutes;
        String line = Main.datasetName + "," + method + "," + seconds + "," + minutes + "," + secondsTotal + "," + minutesTotal + "," + Main.extractMaxCardConstraints + "," + Main.datasetPath;
        log(line);
        System.out.println("Time Elapsed " + method + " " + seconds + " sec , " + minutes + " min");
        System.out.println("***** Total Parsing Time " + secondsTotal + " sec , " + minutesTotal + " min *****");
    }
}
