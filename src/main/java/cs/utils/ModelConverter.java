package cs.utils;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

public class ModelConverter {

    public static org.eclipse.rdf4j.model.Model convertJenaModelToRDF4JModel(Model jenaModel) {
        org.eclipse.rdf4j.model.Model rdf4jModel = new LinkedHashModel();

        for (Statement stmt : jenaModel.listStatements().toList()) {
            Resource subj = convertResource(stmt.getSubject());
            org.eclipse.rdf4j.model.IRI pred = convertIRI(stmt.getPredicate());
            Value obj = convertValue(stmt.getObject());

            rdf4jModel.add(subj, pred, obj);
        }

        return rdf4jModel;
    }

    private static Resource convertResource(org.apache.jena.rdf.model.Resource jenaResource) {
        if (jenaResource.isAnon()) {
            return Values.bnode(jenaResource.getId().getLabelString());
        } else {
            return Values.iri(jenaResource.getURI());
        }
    }

    private static org.eclipse.rdf4j.model.IRI convertIRI(org.apache.jena.rdf.model.Property jenaProperty) {
        return Values.iri(jenaProperty.getURI());
    }

    private static Value convertValue(org.apache.jena.rdf.model.RDFNode jenaNode) {
        if (jenaNode.isLiteral()) {
            org.apache.jena.rdf.model.Literal jenaLiteral = jenaNode.asLiteral();
            String lexicalValue = jenaLiteral.getLexicalForm();
            String datatypeURI = jenaLiteral.getDatatypeURI();

            if (datatypeURI != null) {
                IRI datatype = Values.iri(datatypeURI);
                return Values.literal(lexicalValue, datatype);
            } else {
                return Values.literal(lexicalValue);
            }
        } else if (jenaNode.isAnon()) {
            return Values.bnode(jenaNode.asResource().getId().getLabelString());
        } else {
            return Values.iri(jenaNode.asResource().getURI());
        }
    }
}
