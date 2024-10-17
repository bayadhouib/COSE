package cs.utils;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.apache.jena.shacl.validation.ReportEntry;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.*;


public class SHACLValidator {

    private static final String INTEGER_TYPE_URI = "http://www.w3.org/2001/XMLSchema#integer";
    private static final String INT_TYPE_URI = "http://www.w3.org/2001/XMLSchema#int";
    private static final TypeMapper typeMapper = TypeMapper.getInstance();

    public List<ReportEntry> validateModel(Model dataModel, Model shapesModel) {
        StopWatch watch = new StopWatch();
        watch.start();

        Shapes shapes = Shapes.parse(shapesModel);
        ValidationReport report = ShaclValidator.get().validate(shapes, dataModel.getGraph());

        watch.stop();
        Utils.logTime("::: ChangesValidator", TimeUnit.MILLISECONDS.toSeconds(watch.getTime()), TimeUnit.MILLISECONDS.toMinutes(watch.getTime()));

        return new ArrayList<>(report.getEntries());
    }

    public Model convertRdf4jModelToJena(org.eclipse.rdf4j.model.Model rdf4jModel) {
        Model jenaModel = ModelFactory.createDefaultModel();
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Void>> futures = new ArrayList<>();

        for (Statement stmt : rdf4jModel) {
            futures.add(executor.submit(() -> {
                try {
                    jenaModel.add(
                            jenaModel.createResource(stmt.getSubject().stringValue()),
                            jenaModel.createProperty(stmt.getPredicate().stringValue()),
                            convertLiteral(stmt.getObject())
                    );
                } catch (Exception e) {
                    // Log and ignore bad URIs or other issues
                    System.err.println("Invalid statement encountered: " + stmt);
                }
                return null;
            }));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        return jenaModel;
    }

    private RDFNode convertLiteral(org.eclipse.rdf4j.model.Value value) {
        if (value instanceof org.eclipse.rdf4j.model.Literal) {
            org.eclipse.rdf4j.model.Literal literal = (org.eclipse.rdf4j.model.Literal) value;
            String dataType = literal.getDatatype().stringValue();
            String lexicalForm = literal.getLabel();

            if (INTEGER_TYPE_URI.equals(dataType) || INT_TYPE_URI.equals(dataType)) {
                try {
                    return ResourceFactory.createTypedLiteral(Integer.parseInt(lexicalForm));
                } catch (NumberFormatException e) {
                    return ResourceFactory.createTypedLiteral(lexicalForm, typeMapper.getSafeTypeByName(dataType));
                }
            }
            return ResourceFactory.createTypedLiteral(lexicalForm, typeMapper.getSafeTypeByName(dataType));
        }
        return ResourceFactory.createPlainLiteral(value.stringValue());
    }
}
