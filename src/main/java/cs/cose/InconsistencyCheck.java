package cs.cose;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import java.util.Map;
import java.util.Set;
import cs.utils.ShapeUtils;

public class InconsistencyCheck {

    public void checkInconsistencies(Model oldShapes, Model newShapes, String outputFilename) {
        Map<String, Set<Statement>> oldShapeMap = ShapeUtils.groupStatementsByShape(oldShapes);
        Map<String, Set<Statement>> newShapeMap = ShapeUtils.groupStatementsByShape(newShapes);
        ShapeUtils.writeInconsistenciesToFile(oldShapeMap, newShapeMap, outputFilename);
    }
}
