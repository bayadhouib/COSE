package cs.utils;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;

import java.io.FileInputStream;
import java.io.IOException;

public class SHACLShapeConverter {

    public static Model convertSHACLShapesFile(String filePath) throws IOException {
        Model jenaModel = ModelFactory.createDefaultModel();

        try (FileInputStream inputStream = new FileInputStream(filePath)) {
            RDFParser.create()
                    .source(inputStream)
                    .lang(Lang.TTL)
                    .parse(jenaModel);
        }

        return jenaModel;
    }


}
