package org.apache.marmotta.kiwi.sparql.geosparql.connectionToPostgres;

import org.apache.marmotta.kiwi.model.rdf.KiWiUriResource;
import org.apache.marmotta.kiwi.persistence.KiWiConnection;
import org.apache.marmotta.kiwi.persistence.pgsql.PostgreSQLDialect;
import org.apache.marmotta.kiwi.sail.KiWiValueFactory;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;

import java.sql.*;

import java.lang.UnsupportedOperationException;

public class evaluateWithoutIndexing {

    // Function that returns the pure geometry from an argument
    private String parseToString(Value arg) {
        String[] geoData = arg.toString().split("\\^\\^", 2);
        return geoData[0].replace("\"", "'");
    }

    // Variable that cointains the result of Postgres from Federated query.
    protected String result = "";

    public Value evaluateQuery(String sqlFunctionGeo, ValueFactory valueFactory) throws ValueExprEvaluationException {
        // Create and cast the connection from valueFactory
        KiWiValueFactory modVF = (KiWiValueFactory) valueFactory;
        // Then, get the connection in order to use it for evaluate geosparql federated query
        KiWiConnection kc = modVF.aqcuireConnection();
        // Try to map geometries to SQL Postgres dialect
        try {
            // Establish connection with database
            Connection pgc = kc.getJDBCConnection();
            // Build the final SQL Postgres query
            String builtQuery = "SELECT " + sqlFunctionGeo + " as resultado";
            // Try to extract the result from Postgres response
            try {
                PreparedStatement queryStatement = pgc.prepareStatement(builtQuery);
                ResultSet results = queryStatement.executeQuery();
                ResultSetMetaData rsmd = results.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (results.next()) {
                    for (int i = 1; i <= columnsNumber; i++) {
                        result = results.getString(i);
                    }
                }

            } catch (SQLException e) {
                System.out.println("Error queryStatement: " + e.toString());
                e.printStackTrace();
            }

            // The connection with the database is released
            pgc.close();
            modVF.releaseConnection(kc);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        // Due to this is a GeoSPARQL processing, the result needs to be of type geo:wktLiteral
        URI type = new KiWiUriResource("http://www.opengis.net/ont/geosparql#wktLiteral");
        Value valueResult = new LiteralImpl(result, type);
        return valueResult;
    }
}
