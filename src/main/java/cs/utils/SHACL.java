package cs.utils;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class SHACL {
    public static final String uri = "http://www.w3.org/ns/shacl#";

    protected static final Resource resource(String local) {
        return ResourceFactory.createResource(uri + local);
    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }

    public static final Resource NodeShape = resource("NodeShape");
    public static final Resource PropertyShape = resource("PropertyShape");

    public static final Property targetClass = property("targetClass");
    public static final Property property = property("property");
    public static final Property path = property("path");
    public static final Property datatype = property("datatype");
    public static final Property minCount = property("minCount");
    public static final Property maxCount = property("maxCount");
    public static final Property NodeKind = property("nodeKind");
    public static final Property minExclusive = property("minExclusive");
    public static final Property maxExclusive = property("maxExclusive");
    public static final Property pattern = property("pattern");
    public static final Property minLength = property("minLength");
    public static final Property maxLength = property("maxLength");
    public static final Property node = property("node");
    public static final Property in = property("in");
    public static final Property or = property("or");
    public static final Property hasValue = property("hasValue");
    public static final Property support = property("support");
    public static final Property confidence = property("confidence");
}
