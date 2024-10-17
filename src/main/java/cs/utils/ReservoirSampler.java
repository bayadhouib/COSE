package cs.utils;

import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirSampler {

    // Standard Reservoir Sampling
    public static List<Resource> standardReservoirSampling(Model graph, Resource type, int sampleSize) {
        List<Resource> reservoir = new ArrayList<>(sampleSize);
        Random random = new Random();
        int count = 0;

        StmtIterator iter = graph.listStatements(null, RDF.type, type);
        while (iter.hasNext()) {
            Statement stmt = iter.nextStatement();
            Resource entity = stmt.getSubject();
            if (count < sampleSize) {
                reservoir.add(entity);
            } else {
                int r = random.nextInt(count + 1);
                if (r < sampleSize) {
                    reservoir.set(r, entity);
                }
            }
            count++;
        }

        return reservoir;
    }

//    // Neighbor-based Reservoir Sampling
//    public static List<Resource> neighborBasedReservoirSampling(Model graph, Resource type, int sampleSize) {
//        List<Resource> reservoir = new ArrayList<>(sampleSize);
//        Random random = new Random();
//        int count = 0;
//
//        StmtIterator iter = graph.listStatements(null, RDF.type, type);
//        while (iter.hasNext()) {
//            Statement stmt = iter.nextStatement();
//            Resource entity = stmt.getSubject();
//            if (count < sampleSize) {
//                reservoir.add(entity);
//                // Add neighbors
//                addNeighborsToReservoir(graph, reservoir, entity, sampleSize, random);
//            } else {
//                int r = random.nextInt(count + 1);
//                if (r < sampleSize) {
//                    reservoir.set(r, entity);
//                    // Add neighbors
//                    addNeighborsToReservoir(graph, reservoir, entity, sampleSize, random);
//                }
//            }
//            count++;
//        }
//
//        return reservoir;
//    }

//    private static void addNeighborsToReservoir(Model graph, List<Resource> reservoir, Resource entity, int sampleSize, Random random) {
//        StmtIterator iter = graph.listStatements(entity, null, (RDFNode) null);
//        while (iter.hasNext()) {
//            Statement stmt = iter.nextStatement();
//            RDFNode object = stmt.getObject();
//            if (object.isResource() && !reservoir.contains(object.asResource())) {
//                if (reservoir.size() < sampleSize) {
//                    reservoir.add(object.asResource());
//                } else {
//                    int r = random.nextInt(reservoir.size() + 1);
//                    if (r < sampleSize) {
//                        reservoir.set(r, object.asResource());
//                    }
//                }
//            }
//        }
//    }
}
