package cs.utils;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.*;

public class SHACLShapesParser {

    public static Model parseSHACLFile(String shaclFilePath) throws IOException, RDFParseException, RDFHandlerException {
        // Create RDF Parser and Handler
        RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
        Model model = new LinkedHashModel();

        // Setup parser and handler
        rdfParser.setRDFHandler(new StatementCollector(model));

        // Parse the SHACL file
        try (InputStream inputStream = new FileInputStream(shaclFilePath)) {
            rdfParser.parse(inputStream, "");
        }

        return model;
    }
}
