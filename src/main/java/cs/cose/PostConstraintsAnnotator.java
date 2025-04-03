package cs.cose;

import cs.utils.FilesUtil;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryConnection;

public class PostConstraintsAnnotator {
    private final RepositoryConnection conn;


    public PostConstraintsAnnotator(RepositoryConnection conn) {
        System.out.println("Initializing PostConstraintsAnnotator...");
        this.conn = conn;
    }


    public void addShNodeConstraint() {
        System.out.println("Adding sh:node constraints to SHACL shapes...");
        getNodeShapesAndIterativelyProcessPropShapes();
    }


    private void getNodeShapesAndIterativelyProcessPropShapes() {
        TupleQuery query = conn.prepareTupleQuery(FilesUtil.readShaclQuery("node_shapes"));
        try (TupleQueryResult result = query.evaluate()) {
            while (result.hasNext()) {
                BindingSet solution = result.next();
                String nodeShape = solution.getValue("nodeShape").stringValue();
                getPropShapesWithDirectShClassAttribute(nodeShape);
                getPropShapesWithEncapsulatedShClassAttribute(nodeShape);
            }
        } catch (Exception e) {
            System.err.println("Error processing node shapes: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private void getPropShapesWithDirectShClassAttribute(String nodeShape) {
        TupleQuery query = conn.prepareTupleQuery(FilesUtil.readShaclQuery("ps_of_ns_direct_sh_class").replace("NODE_SHAPE", nodeShape));
        try (TupleQueryResult result = query.evaluate()) {
            while (result.hasNext()) {
                BindingSet solution = result.next();
                insertShNodeConstraint(solution);
            }
        } catch (Exception e) {
            System.err.println("Error processing direct sh:class attributes for node shape " + nodeShape + ": " + e.getMessage());
            e.printStackTrace();
        }
    }


    private void getPropShapesWithEncapsulatedShClassAttribute(String nodeShape) {
        TupleQuery queryA = conn.prepareTupleQuery(FilesUtil.readShaclQuery("ps_of_ns_indirect_sh_class").replace("NODE_SHAPE", nodeShape));
        try (TupleQueryResult resultA = queryA.evaluate()) {
            while (resultA.hasNext()) {
                BindingSet bindingsA = resultA.next();
                String propertyShape = bindingsA.getValue("propertyShape").stringValue();
                TupleQuery queryB = conn.prepareTupleQuery(FilesUtil.readShaclQuery("sh_class_indirect_ps")
                        .replace("PROPERTY_SHAPE", propertyShape)
                        .replace("NODE_SHAPE", nodeShape));
                try (TupleQueryResult resultB = queryB.evaluate()) {
                    while (resultB.hasNext()) {
                        BindingSet bindingsB = resultB.next();
                        insertShNodeConstraint(bindingsB);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing encapsulated sh:class attributes for node shape " + nodeShape + ": " + e.getMessage());
            e.printStackTrace();
        }
    }


    private void insertShNodeConstraint(BindingSet bindings) {
        String classVal = bindings.getValue("class").stringValue();
        if (classVal.contains("<")) {
            classVal = classVal.substring(1, classVal.length() - 1);
        }

        if (isNodeShape(classVal)) {
            String propertyShape = bindings.getValue("propertyShape").stringValue();
            String insertStatement = "INSERT { <" + propertyShape + "> <" + SHACL.NODE + "> <" + getNodeShape(classVal) + "> } " +
                    "WHERE { <" + propertyShape + "> a <http://www.w3.org/ns/shacl#PropertyShape> . }";
            try {
                Update updateQuery = conn.prepareUpdate(insertStatement);
                updateQuery.execute();
            } catch (Exception e) {
                System.err.println("Error inserting sh:node constraint for property shape " + propertyShape + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }


    private boolean isNodeShape(String shaclClassValue) {
        String query = FilesUtil.readShaclQuery("ns_existence").replace("SHACL_CLASS", shaclClassValue);
        try {
            return conn.prepareBooleanQuery(query).evaluate();
        } catch (Exception e) {
            System.err.println("Error checking if " + shaclClassValue + " is a node shape: " + e.getMessage());
            return false;
        }
    }


    private String getNodeShape(String shaclClassValue) {
        String nodeShapeIRI = "";
        String query = FilesUtil.readShaclQuery("ns").replace("SHACL_CLASS", shaclClassValue);
        try {
            TupleQuery tupleQuery = conn.prepareTupleQuery(query);
            try (TupleQueryResult result = tupleQuery.evaluate()) {
                if (result.hasNext()) {
                    BindingSet solution = result.next();
                    nodeShapeIRI = solution.getValue("nodeShape").stringValue();
                }
            }
        } catch (Exception e) {
            System.err.println("Error retrieving node shape IRI for " + shaclClassValue + ": " + e.getMessage());
        }
        return nodeShapeIRI;
    }
}
