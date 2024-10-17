package cs.cose;

import cs.utils.SupportConfidence;
import cs.utils.Tuple3;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SHACL;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.function.BiPredicate;

public class ImpactAnalysis {

    private final Map<Integer, List<Integer>> sampledEntitiesPerClass;
    private final Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes;
    private final Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport;
    private final Map<Integer, Integer> classEntityCount;
    private static final Logger logger = Logger.getLogger(ImpactAnalysis.class.getName());

    public ImpactAnalysis(Map<Integer, List<Integer>> sampledEntitiesPerClass, Map<Integer, Map<Integer, Set<Integer>>> classToPropWithObjTypes, Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> shapeTripletSupport, Map<Integer, Integer> classEntityCount) {
        this.sampledEntitiesPerClass = sampledEntitiesPerClass;
        this.classToPropWithObjTypes = classToPropWithObjTypes;
        this.shapeTripletSupport = shapeTripletSupport;
        this.classEntityCount = classEntityCount;
    }

    public boolean isChangeAffectingShapes(ChangeDetection.Change change, Model shapesModel) {
        return shapesModel.subjects().stream().anyMatch(shape -> isShapeAffected(shape, change, shapesModel));
    }

    public Set<Resource> identifyAffectedShapes(List<ChangeDetection.Change> changes, Model shapesModel) {
        Set<Resource> affectedShapes = new HashSet<>();
        for (ChangeDetection.Change change : changes) {
            for (Resource shape : shapesModel.subjects()) {
                if (isShapeAffected(shape, change, shapesModel)) {
                    logger.info("Shape affected: " + shape + " by change: " + change);
                    affectedShapes.add(shape);
                }
            }
        }
        return affectedShapes;
    }

    private boolean isShapeAffected(Resource shapeNode, ChangeDetection.Change change, Model model) {
        Map<IRI, BiPredicate<Resource, ChangeDetection.Change>> constraintMatchers = Map.ofEntries(
                Map.entry(SHACL.TARGET_CLASS, (shape, chg) -> matchesTargetClass(shape, chg, model)),
                Map.entry(SHACL.TARGET_NODE, (shape, chg) -> matchesTargetNode(shape, chg, model)),
                Map.entry(SHACL.TARGET_SUBJECTS_OF, (shape, chg) -> matchesTargetSubjectsOf(shape, chg, model)),
                Map.entry(SHACL.TARGET_OBJECTS_OF, (shape, chg) -> matchesTargetObjectsOf(shape, chg, model)),
                Map.entry(SHACL.PROPERTY, (shape, chg) -> matchesPropertyConstraints(shape, chg, model)),
                Map.entry(SHACL.OR, (shape, chg) -> matchesOrConstraint(shape, chg, model)),
                Map.entry(SHACL.AND, (shape, chg) -> matchesAndConstraint(shape, chg, model)),
                Map.entry(SHACL.NOT, (shape, chg) -> matchesNotConstraint(shape, chg, model)),
                Map.entry(SHACL.XONE, (shape, chg) -> matchesXoneConstraint(shape, chg, model)),
                Map.entry(SHACL.CLASS, (shape, chg) -> matchesClassConstraint(shape, chg, model)),
                Map.entry(SHACL.HAS_VALUE, (shape, chg) -> matchesHasValueConstraint(shape, chg, model)),
                Map.entry(SHACL.IN, (shape, chg) -> matchesInConstraint(shape, chg, model)),
                Map.entry(SHACL.INVERSE_PATH, (shape, chg) -> matchesInversePathConstraint(shape, chg, model))
        );

        return constraintMatchers.entrySet().stream()
                .anyMatch(entry -> {
                    boolean result = Models.getPropertyResources(model, shapeNode, entry.getKey()).stream()
                            .anyMatch(propertyShape -> entry.getValue().test(propertyShape, change));
                    if (result) {
                        System.out.println("Shape " + shapeNode + " affected by change: " + change + " on constraint: " + entry.getKey());
                    }
                    return result;
                });
    }

    private boolean matchesTargetClass(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.TARGET_CLASS).stream()
                .anyMatch(targetClass -> {
                    Resource entityResource = SimpleValueFactory.getInstance().createIRI(change.entity);
                    boolean match = Models.object(model.filter(entityResource, RDF.TYPE, (Resource) targetClass)).isPresent();
                    if (match) {
                        System.out.println("Matched target class constraint for shape " + shape + " and change: " + change);
                    }
                    return match;
                });
        if (!result) {
            System.out.println("No match for target class constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesTargetNode(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.TARGET_NODE)
                .contains(SimpleValueFactory.getInstance().createIRI(change.entity));
        if (result) {
            System.out.println("Matched target node constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for target node constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesTargetSubjectsOf(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.TARGET_SUBJECTS_OF).stream()
                .anyMatch(subjectsOf -> change.property.equals(subjectsOf.toString()));
        if (result) {
            System.out.println("Matched target subjects of constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for target subjects of constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesTargetObjectsOf(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.TARGET_OBJECTS_OF).stream()
                .anyMatch(objectsOf -> change.property.equals(objectsOf.toString()));
        if (result) {
            System.out.println("Matched target objects of constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for target objects of constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesPropertyConstraints(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.PROPERTY).stream()
                .anyMatch(propertyShape -> {
                    Optional<Value> pathNode = Models.object(model.filter(propertyShape, SHACL.PATH, null));
                    boolean match = pathNode.isPresent() && change.property.equals(pathNode.get().toString()) && checkValueConstraints(propertyShape, change.newValue, model);
                    if (match) {
                        System.out.println("Matched property constraint for shape " + shape + " and change: " + change);
                    }
                    return match;
                });
        if (!result) {
            System.out.println("No match for property constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean checkValueConstraints(Resource propertyShape, String newValue, Model model) {
        boolean valid = true;

        if (Models.objectIRI(model.filter(propertyShape, SHACL.DATATYPE, null))
                .map(datatype -> !isValidDatatype(newValue, datatype.toString())).orElse(false)) {
            valid = false;
        }

        // Additional checks for other constraints can be added here

        if (!valid) {
            System.out.println("Value constraints failed for newValue: " + newValue + " with shape: " + propertyShape);
        }
        return valid;
    }

    private boolean isValidDatatype(String value, String datatype) {
        try {
            switch (datatype) {
                case "http://www.w3.org/2001/XMLSchema#integer":
                    Integer.parseInt(value);
                    break;
                case "http://www.w3.org/2001/XMLSchema#float":
                    Float.parseFloat(value);
                    break;
                case "http://www.w3.org/2001/XMLSchema#double":
                    Double.parseDouble(value);
                    break;
                case "http://www.w3.org/2001/XMLSchema#string":
                    break;
                default:
                    return false;
            }
        } catch (NumberFormatException e) {
            logger.warning("Invalid datatype for value: " + value + " with expected datatype: " + datatype);
            return false;
        }
        return true;
    }

    private boolean matchesOrConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.OR).stream()
                .anyMatch(orShape -> isShapeAffected(orShape, change, model));
        if (result) {
            System.out.println("Matched OR constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for OR constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesAndConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.AND).stream()
                .allMatch(andShape -> isShapeAffected(andShape, change, model));
        if (result) {
            System.out.println("Matched AND constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for AND constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesNotConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.NOT).stream()
                .noneMatch(notShape -> isShapeAffected(notShape, change, model));
        if (result) {
            System.out.println("Matched NOT constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for NOT constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesXoneConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        long matchCount = Models.getPropertyResources(model, shape, SHACL.XONE).stream()
                .filter(xoneShape -> isShapeAffected(xoneShape, change, model))
                .count();
        boolean result = matchCount == 1;
        if (result) {
            System.out.println("Matched XONE constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for XONE constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesClassConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.object(model.filter(shape, SHACL.CLASS, null))
                .map(classNode -> {
                    Resource entityResource = SimpleValueFactory.getInstance().createIRI(change.entity);
                    boolean match = Models.object(model.filter(entityResource, RDF.TYPE, (Resource) classNode)).isPresent();
                    if (match) {
                        System.out.println("Matched class constraint for shape " + shape + " and change: " + change);
                    }
                    return match;
                })
                .orElse(false);
        if (!result) {
            System.out.println("No match for class constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesHasValueConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.object(model.filter(shape, SHACL.HAS_VALUE, null))
                .map(valueNode -> change.newValue.equals(valueNode.toString()))
                .orElse(false);
        if (result) {
            System.out.println("Matched HAS VALUE constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for HAS VALUE constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesInConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.getPropertyResources(model, shape, SHACL.IN).stream()
                .anyMatch(node -> change.newValue.equals(node.toString()));
        if (result) {
            System.out.println("Matched IN constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for IN constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }

    private boolean matchesInversePathConstraint(Resource shape, ChangeDetection.Change change, Model model) {
        boolean result = Models.object(model.filter(shape, SHACL.INVERSE_PATH, null))
                .map(pathNode -> change.property.equals(pathNode.toString()))
                .orElse(false);
        if (result) {
            System.out.println("Matched INVERSE PATH constraint for shape " + shape + " and change: " + change);
        } else {
            System.out.println("No match for INVERSE PATH constraint for shape " + shape + " and change: " + change);
        }
        return result;
    }
}
