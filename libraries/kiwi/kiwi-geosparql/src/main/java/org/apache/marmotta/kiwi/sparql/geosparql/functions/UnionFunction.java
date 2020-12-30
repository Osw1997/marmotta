/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.marmotta.kiwi.sparql.geosparql.functions;

import info.aduna.iteration.CloseableIteration;
import org.apache.marmotta.kiwi.caching.CacheManager;
import org.apache.marmotta.kiwi.caching.CacheManagerFactory;
import org.apache.marmotta.kiwi.config.KiWiConfiguration;
import org.apache.marmotta.kiwi.model.rdf.KiWiUriResource;
import org.apache.marmotta.kiwi.persistence.KiWiDialect;
import org.apache.marmotta.kiwi.persistence.KiWiPersistence;
import org.apache.marmotta.kiwi.persistence.pgsql.PostgreSQLDialect;
import org.apache.marmotta.kiwi.reasoner.persistence.KiWiReasoningConnection;
import org.apache.marmotta.kiwi.reasoner.persistence.KiWiReasoningPersistence;
import org.apache.marmotta.kiwi.sail.KiWiSailConnection;
import org.apache.marmotta.kiwi.sail.KiWiStore;
import org.apache.marmotta.kiwi.sail.KiWiValueFactory;
import org.apache.marmotta.kiwi.sparql.builder.ValueType;
import org.apache.marmotta.kiwi.sparql.function.NativeFunction;
import org.apache.marmotta.kiwi.vocabulary.FN_GEOSPARQL;


//import org.apache.marmotta.platform.backend.kiwi.KiWiOptions;
//import org.apache.marmotta.platform.backend.kiwi.KiWiStoreProvider;


import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;

import org.apache.marmotta.kiwi.persistence.KiWiConnection;
import org.apache.marmotta.platform.core.api.config.ConfigurationService;

import org.apache.marmotta.kiwi.sparql.geosparql.connectionToPostgres.evaluateWithoutIndexing;

import java.sql.*;

/**
 * A SPARQL function for doing a union between two geometries. Should be
 * implemented directly in the database, as the in-memory implementation is
 * non-functional. Only support by postgres - POSTGIS
 * <p/>
 * The function can be called either as:
 * <ul>
 *      <li>geof:union(?geometryA, ?geometryB) </li>
 * </ul>
 * Its necesary enable postgis in your database with the next command "CREATE
 * EXTENSION postgis;" Note that for performance reasons it might be preferrable
 * to create a geometry index for your database. Please consult your database
 * documentation on how to do this.
 *
 * @author Xavier Sumba (xavier.sumba93@ucuenca.ec))
 */
public class UnionFunction implements NativeFunction {

    // auto-register for SPARQL environment
    static {
        if (!FunctionRegistry.getInstance().has(FN_GEOSPARQL.UNION.toString())) {
            FunctionRegistry.getInstance().add(new UnionFunction());
        }
    }

    // Function that returns the pure geometry from an argument
    private String parseToString(Value arg) {
        String[] geoData = arg.toString().split("\\^\\^", 2);
        return geoData[0].replace("\"", "'");
    }

    // protected String result = "";

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {

        // Create a new Postgres dialect
        PostgreSQLDialect dialect = new PostgreSQLDialect();
        // Get pure geometries from arguments
        String arg1 = parseToString(args[0]);
        String arg2 = parseToString(args[1]);

        // Translate the geometries into Postgres dialect
        String sqlFunctionGeo = this.getNative(dialect, arg1, arg2);

        evaluateWithoutIndexing q = new evaluateWithoutIndexing();

        return q.evaluateQuery(sqlFunctionGeo, valueFactory);



//        PostgreSQLDialect dialect = new PostgreSQLDialect();
//        KiWiValueFactory modVF = (KiWiValueFactory) valueFactory;
//
//        KiWiConnection kc = modVF.aqcuireConnection();
//        try {
//            Connection pgc = kc.getJDBCConnection();
//
//            String arg1 = parseToString(args[0]);
//            String arg2 = parseToString(args[1]);
//            System.out.println("Value 1 is: " + arg1);
//            System.out.println("Value 2 is: " + arg2);
//
//
//            //String sqlFunctionGeo = this.getNative((KiWiDialect) PostgreSQLDialect, arg1, arg2);
//            String sqlFunctionGeo = this.getNative(dialect, arg1, arg2);
//            System.out.println("Parsed Geo: " + sqlFunctionGeo);
//
//            String builtQuery = "SELECT " + sqlFunctionGeo + " as resultado";
//            System.out.println("Final query: " + builtQuery);
//
//
//            try {
//                PreparedStatement queryStatement = pgc.prepareStatement(builtQuery);
//                /*
//                System.out.println("Final: " + queryStatement.toString());
//                System.out.println("Tipo: " + queryStatement.getClass().getName());
//                System.out.println(queryStatement);
//                 */
//
//                ResultSet results = queryStatement.executeQuery();
//                /*
//                System.out.println("Final: " + results.toString());
//                System.out.println("Tipo: " + results.toString());
//                System.out.println(results);
//                 */
//
//                //result = results.getString(0);
//                //System.out.println("Resultado: " + result);
//
//
//                ResultSetMetaData rsmd = results.getMetaData();
//                int columnsNumber = rsmd.getColumnCount();
//                while (results.next()) {
//                    for (int i = 1; i <= columnsNumber; i++) {
//                            result = results.getString(i);
//                        System.out.println("Resultado: " + result + " || " + rsmd.getColumnName(i));
//                    }
//                    System.out.println("");
//                }
//
//
//                System.out.println("FIN :)");
//            } catch (SQLException e) {
//                System.out.println("Error queryStatement: " + e.toString());
//                e.printStackTrace();
//            }
//
//            // Se cierra la conexion
//            pgc.close();
//            modVF.releaseConnection(kc);
//
//            URI type = new KiWiUriResource("http://www.opengis.net/ont/geosparql#wktLiteral");
//            Value valueResult = new LiteralImpl(result, type);
//
//            return valueResult;
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        //throw new UnsupportedOperationException("cannot evaluate in-memory, needs to be supported by the database");
    }

    @Override
    public String getURI() {
        return FN_GEOSPARQL.UNION.toString();
    }

    /**
     * Return true if this function has available native support for the given
     * dialect
     *
     * @param dialect
     * @return
     */
    @Override
    public boolean isSupported(KiWiDialect dialect) {
        return dialect instanceof PostgreSQLDialect;
    }

    /**
     * Return a string representing how this GeoSPARQL function is translated
     * into SQL ( Postgis Function ) in the given dialect
     *
     * @param dialect
     * @param args
     * @return
     */
    @Override
    public String getNative(KiWiDialect dialect, String... args) {
        if (dialect instanceof PostgreSQLDialect) {
            if (args.length == 2) {
                String geom1 = args[0];
                String geom2 = args[1];
                String SRID_default = "4326";
                /*
                 * The following condition is required to read WKT  inserted directly into args[0] or args[1] and create a geometries with SRID
                 * POINT, MULTIPOINT, LINESTRING ... and MULTIPOLYGON conditions: 
                 *   example: geof:union(?geom1, "POLYGON(( -7 43, -2 43, -2 38, -7 38, -7 43))"^^geo:wktLiteral))
                 * st_AsText condition: It is to use the geometry that is the result of another function geosparql.
                 */
                if (args[0].contains("POINT") || args[0].contains("MULTIPOINT") || args[0].contains("LINESTRING") || args[0].contains("MULTILINESTRING") || args[0].contains("POLYGON") || args[0].contains("MULTIPOLYGON") || args[0].contains("ST_AsText")) {
                    geom1 = String.format("ST_GeomFromText(%s,%s)", args[0], SRID_default);
                }
                if (args[1].contains("POINT") || args[1].contains("MULTIPOINT") || args[1].contains("LINESTRING") || args[1].contains("MULTILINESTRING") || args[1].contains("POLYGON") || args[1].contains("MULTIPOLYGON") || args[1].contains("ST_AsText")) {
                    geom2 = String.format("ST_GeomFromText(%s,%s)", args[1], SRID_default);
                }
                return String.format("ST_AsText(ST_Union(%s , %s ) )", geom1, geom2);
            }
        }
        throw new UnsupportedOperationException("union function not supported by dialect " + dialect);
    }

    /**
     * Get the return type of the function. This is needed for SQL type casting
     * inside KiWi.
     *
     * @return
     */
    @Override
    public ValueType getReturnType() {
        return ValueType.GEOMETRY;
    }

    /**
     * Get the argument type of the function for the arg'th argument (starting
     * to count at 0). This is needed for SQL type casting inside KiWi.
     *
     * @param arg
     * @return
     */
    @Override
    public ValueType getArgumentType(int arg) {
        return ValueType.GEOMETRY;
    }

    /**
     * Return the minimum number of arguments this function requires.
     *
     * @return
     */
    @Override
    public int getMinArgs() {
        return 2;
    }

    /**
     * Return the maximum number of arguments this function can take
     *
     * @return
     */
    @Override
    public int getMaxArgs() {
        return 2;
    }
}
