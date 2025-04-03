package cs.utils;

import org.apache.jena.rdf.model.*;
import org.apache.jena.shacl.*;
import org.apache.jena.vocabulary.*;

import java.util.*;

public class SHACLCleaner {

    // Detect and remove cycles in SHACL shapes
    public Model cleanSHACLShapes(Model shapesModel) {
        detectAndRemoveCycles(shapesModel);
        removeRedundantShapes(shapesModel);
        cleanPaths(shapesModel);
        return shapesModel;
    }

    // Detect and remove cycles
    private void detectAndRemoveCycles(Model shapesModel) {
        Set<Resource> cycleDetected = new HashSet<>();
        Map<Resource, NodeState> visitedNodes = new HashMap<>();

        // Iterate over all statements in the model and check for cycles
        StmtIterator propertyShapesIterator = shapesModel.listStatements(null, RDF.type, SHACL.PropertyShape);
        while (propertyShapesIterator.hasNext()) {
            Resource propertyShape = propertyShapesIterator.nextStatement().getSubject();

            // Only process unvisited nodes
            if (!cycleDetected.contains(propertyShape) && hasCycle(propertyShape, visitedNodes, shapesModel)) {
                cycleDetected.add(propertyShape);
                // Remove the cyclic shape
                shapesModel.removeAll(propertyShape, null, null);
            }
        }

        // After removal, recheck for cycles in case removal created new cycles
        if (!cycleDetected.isEmpty()) {
            detectAndRemoveCycles(shapesModel);
        }
    }

    // Check if a specific property shape introduces a cycle (including path constraints)
    private boolean hasCycle(Resource current, Map<Resource, NodeState> visitedNodes, Model shapesModel) {
        NodeState state = visitedNodes.getOrDefault(current, NodeState.UNVISITED);

        if (state == NodeState.VISITING) {
            return true; // Cycle detected
        }
        if (state == NodeState.VISITED) {
            return false; // Already processed
        }

        visitedNodes.put(current, NodeState.VISITING);
        StmtIterator relatedShapes = shapesModel.listStatements(current, SHACL.path, (RDFNode) null);

        // Check all path constraints recursively for cycles
        while (relatedShapes.hasNext()) {
            RDFNode relatedNode = relatedShapes.nextStatement().getObject();
            if (relatedNode.isResource() && hasCycle(relatedNode.asResource(), visitedNodes, shapesModel)) {
                return true;
            }
        }

        // Also check for recursive property shape references (highly nested shapes)
        StmtIterator propertyShapes = shapesModel.listStatements(current, SHACL.property, (RDFNode) null);
        while (propertyShapes.hasNext()) {
            RDFNode propertyShapeNode = propertyShapes.nextStatement().getObject();
            if (propertyShapeNode.isResource() && hasCycle(propertyShapeNode.asResource(), visitedNodes, shapesModel)) {
                return true;
            }
        }

        visitedNodes.put(current, NodeState.VISITED);
        return false;
    }

    // Remove redundant shapes and properties
    private void removeRedundantShapes(Model shapesModel) {
        Set<Resource> seenShapes = new HashSet<>();
        StmtIterator propertyShapesIterator = shapesModel.listStatements(null, RDF.type, SHACL.NodeShape);

        while (propertyShapesIterator.hasNext()) {
            Resource shape = propertyShapesIterator.nextStatement().getSubject();

            // Remove shape if it is redundant (i.e., already seen)
            if (seenShapes.contains(shape)) {
                shapesModel.removeAll(shape, null, null);
            } else {
                seenShapes.add(shape);
            }
        }
    }

    // Clean paths to avoid cycles caused by path constraints
    private void cleanPaths(Model shapesModel) {
        StmtIterator propertyShapesIterator = shapesModel.listStatements(null, SHACL.property, (RDFNode) null);
        while (propertyShapesIterator.hasNext()) {
            Statement propertyShapeStmt = propertyShapesIterator.nextStatement();
            Resource propertyShape = propertyShapeStmt.getObject().asResource();

            // If a property has multiple paths, keep only the first one and remove the others
            List<Statement> paths = shapesModel.listStatements(propertyShape, SHACL.path, (RDFNode) null).toList();
            if (paths.size() > 1) {
                paths.stream().skip(1).forEach(shapesModel::remove); // Keep only the first path
            }
        }
    }

    private enum NodeState {
        UNVISITED,
        VISITING,
        VISITED
    }
}
