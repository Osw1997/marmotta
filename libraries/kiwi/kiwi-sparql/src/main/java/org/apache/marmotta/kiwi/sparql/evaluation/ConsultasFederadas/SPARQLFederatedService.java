package org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.evaluation.federation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.Iterations;
import info.aduna.iteration.SingletonIteration;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.algebra.evaluation.iterator.SilentIteration;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.sparql.query.InsertBindingSetCursor;

/**
* Clase encargada de ejecutar y usar las conexiones, resultados, excepciones y
* errores que se suciten en la creación de una instancia de consulta federada en Apache Marmotta.
*
* Implementa la interfaz FederatedService ya que las 2 opciones de ejecución de consulta
* federada (evaluate(...) >> Simple y vectorizada) por lo que el bloque de código
* de tales métodos se encuentran en esta clase.
*
* */
public class SPARQLFederatedService implements FederatedService {

    // Variable que sirve para escribir los eventos en un archivo LOG.
    final static Logger logger = LoggerFactory.getLogger(org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.SPARQLFederatedService.class);

    /**
    * Clase que lidia con las expresiones de consultas federadas al evaluar
    * los resultados intermedios por lotes y administra todos los resultados generados.
    *
    * Se hace uso de una clase de OpenRDF llamado JoinExecutorBase oara aprovechar sus funcionalidades
    * con el objetivo de tener los resultados finales correctos.
    *
    * */
    private class BatchingServiceIteration extends JoinExecutorBase<BindingSet> {

        // Variable que guarda el valor de lotes, en caso de necesitarlo.
        private final int blockSize;

        // Variable que guarda el objeto de la sesión de conexión al SPARQL endpoint remoto.
        private final Service service;

        // Constructor de la clase que recibe como parámetros una colección de resultados, el tamaño del lote
        // y el objeto que contiene la sesión de la conexión
        public BatchingServiceIteration(CloseableIteration<BindingSet, QueryEvaluationException> inputBindings,
                                        int blockSize, Service service)
                throws QueryEvaluationException
        {
            super(inputBindings, null, EmptyBindingSet.getInstance());
            this.blockSize = blockSize;
            this.service = service;
            //run();
            try {
                handleBindings();
            }
            catch (Exception e) {
                toss(e);
            }
            finally {
                finished = true;
                rightQueue.done();
            }
        }

        // Se sobreescribe el método de la clase padre y por cada lote (batch = 15),
        // se evalua de forma interna cada consulta y poco a poco se van construyendo los resultados
        @Override
        protected void handleBindings()
                throws Exception
        {
            while (!closed && leftIter.hasNext()) {

                ArrayList<BindingSet> blockBindings = new ArrayList<BindingSet>(blockSize);
                for (int i = 0; i < blockSize; i++) {
                    if (!leftIter.hasNext())
                        break;
                    blockBindings.add(leftIter.next());
                }
                CloseableIteration<BindingSet, QueryEvaluationException> materializedIter = new CollectionIteration<BindingSet, QueryEvaluationException>(
                        blockBindings);
                addResult(evaluateInternal(service, materializedIter, service.getBaseURI()));
            }
        }
    }

    // Variable que almacenará información de la conexión al repositorio de datos SPARQL.
    protected final SPARQLRepository rep;

    // Variable que contendrá la conexión al repositorio de datos SPARQL.
    protected RepositoryConnection conn = null;

    // Constructor de la clase SPARQLFederatedService
    public SPARQLFederatedService(String serviceUrl) {
        super();
        this.rep = new SPARQLRepository(serviceUrl);
    }

    /**
     * Con base a la sesión del SPARQL endpoint remoto (service)
     * asociado a la URI pasada como argumento (baseUri),
     * se evalua la consulta (sparqlQueryString)
     * según el tipo de consulta (type = {SELECT, ASK})
     * usando el BindingSet como objeto que delimita las consultas (bindings)
     * y lanza una excepción si ocurre un error en la evaluación de la consulta (QueryEvaluationException).
     *
     * Devuelve una colección (iterable) sobre los resultado que están asociados
     * a los binding suministrados (CloseableIteration<BindingSet, QueryEvaluationException>)
     *
     * Se contempla el hecho de que la consulta puede ser silenciosa (SILENT) por lo que
     * errores o resultados pueden no ser retornados explícitamente.
     *
     * */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(String sparqlQueryString,
                                                                             BindingSet bindings,
                                                                             String baseUri,
                                                                             QueryType type,
                                                                             Service service)
            throws QueryEvaluationException
    {

        try {

            if (type == QueryType.SELECT) {

                // Se prepara la consulta tipo SELECT
                TupleQuery query = getConnection().prepareTupleQuery(QueryLanguage.SPARQL, sparqlQueryString,
                        baseUri);

                // Se crea un objeto que se puede iterar de tipo Binding
                Iterator<Binding> bIter = bindings.iterator();
                // Hasta que se termine el objeto por iterar, se hace el proceso
                while (bIter.hasNext()) {
                    Binding b = bIter.next();
                    // Se verifica que el endpoint remoto tenga el binding solicitado a nivel consulta
                    if (service.getServiceVars().contains(b.getName()))
                        query.setBinding(b.getName(), b.getValue());
                }

                TupleQueryResult res = query.evaluate();

                // Se asocia el resultado con los bindings y se regresa tal asociación.
                return new InsertBindingSetCursor(res, bindings);

            }
            else if (type == QueryType.ASK) {
                BooleanQuery query = getConnection().prepareBooleanQuery(QueryLanguage.SPARQL, sparqlQueryString,
                        baseUri);

                // Codigo por optimizar ¿?
                Iterator<Binding> bIter = bindings.iterator();
                while (bIter.hasNext()) {
                    Binding b = bIter.next();
                    if (service.getServiceVars().contains(b.getName()))
                        query.setBinding(b.getName(), b.getValue());
                }

                boolean exists = query.evaluate();

                // check if triples are available (with inserted bindings)
                if (exists)
                    return new SingletonIteration<BindingSet, QueryEvaluationException>(bindings);
                else
                    return new EmptyIteration<BindingSet, QueryEvaluationException>();
            }
            else
                throw new QueryEvaluationException("Unsupported QueryType: " + type.toString());

        }
        catch (MalformedQueryException e) {
            throw new QueryEvaluationException(e);
        }
        catch (RepositoryException e) {
            throw new QueryEvaluationException("SPARQLRepository for endpoint " + rep.toString()
                    + " could not be initialized.", e);
        }
    }

    /**
     * Con base a la sesión del SPARQL endpoint remoto (service)
     * asociado a la URI pasada como argumento (baseUri),
     * usando la colección de resultados en CloseableIteration como objeto que delimita las consultas (bindings)
     * considerando una consulta vectorizada o múltiple
     * y lanza una excepción si ocurre un error en la evaluación de la consulta (QueryEvaluationException).
     *
     * Devuelve una colección (iterable) sobre los resultado que están asociados
     * a los bindings suministrados originales (CloseableIteration<BindingSet, QueryEvaluationException>)
     *
     * Se contempla el hecho de que la consulta puede ser silenciosa (SILENT) por lo que
     * errores o resultados pueden no ser retornados explícitamente.
     *
     * Al ser una consulta vectorizada, se evaluan los bindings por lote, que en este caso son 15, y luego
     * se evaluan tales bindings de manera interna en el método evaluateInternal.
     *
     * */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
                                                                             CloseableIteration<BindingSet, QueryEvaluationException> bindings,
                                                                             String baseUri)
            throws QueryEvaluationException
    {

        // Variable que indica el número de bindings posibles
        int blockSize = 15;

        if (blockSize > 0) {
            return new SPARQLFederatedService.BatchingServiceIteration(bindings, blockSize, service);
        }
        else {
            return evaluateInternal(service, bindings, service.getBaseURI());
        }
    }

    /**
    * Continuación del método anterior (evaluate) y es la continuación ya que este método se encarga de evaluar
    * la consulta y usa los bindings como delimitadores de la consulta.
    *
    * Esta método es llamado por la clase anidada BatchingServiceIteration
    * alojada en esta clase (SPARQLFederatedService)
    *
    * */
    protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateInternal(Service service,
                                                                                        CloseableIteration<BindingSet, QueryEvaluationException> bindings,
                                                                                        String baseUri)
            throws QueryEvaluationException
    {

        // Crea una variable que contenga todas los bindings del batch para que
        // en caso de fallar la consulta respectiva del batch, ya que al asociar los binding al resultado del batch,
        // se pueda continuar con los bindings pendientes sin detener el proceso.
        List<BindingSet> allBindings = new LinkedList<BindingSet>();
        while (bindings.hasNext()) {
            allBindings.add(bindings.next());
        }

        // Si la cantindad de bindings conrrespondientes al batch es cero, retorna un objeto vacío.
        if (allBindings.size() == 0) {
            return new EmptyIteration<BindingSet, QueryEvaluationException>();
        }

        // Se crea un conjunto de variables que están presentes en consultas y en subconsultas del servicio
        // al mismo tiempo las cuales son denominadas variables de proyección (proyectionVars)
        Set<String> projectionVars = new HashSet<String>(service.getServiceVars());
        // Con el fin de no tener bindings repetidos, se eliminan los bindings que también estén en allBindings.
        projectionVars.removeAll(allBindings.get(0).getBindingNames());

        // below we need to take care for SILENT services
        // Se crea una variable que contendrá los resultados y que estarán asociados a los bindings del batch actual.
        CloseableIteration<BindingSet, QueryEvaluationException> result = null;
        // Se intenta evaluar la consulta
        try {
            // fallback to simple evaluation (just a single binding)
            // Si la cantidad de bindgins es uno, implica ser una consulta simple
            // por lo que no se necesitarán variables de proyección
            if (allBindings.size() == 1) {
                // Extrae la consulta proveniente del servicio asociada a las variables de proyección.
                String queryString = service.getQueryString(projectionVars);
                // Se evalua la consulta usando el conjunto de bindings, el tipo de consulta
                // y el objeto que contiene la conexión (SERVICE).
                result = evaluate(queryString, allBindings.get(0), baseUri, QueryType.SELECT, service);
                // Si el la consulta está declarada como silenciosa, no se asigna el resultado de manera explícita
                // y si no, el resultado no se encapsula en un objeto.
                result = service.isSilent() ? new SilentIteration(result) : result;
                // Se regresa el resultado.
                return result;
            }

            // Al no ser una consulta cuyos bindings impliquen una consulta simple,
            // se necesita algo que identifique a cada fila de cada batch en caso
            // de que los bindings se necesiten ocupar nuevamente. Se soluciona anexando la palabra "__rowIdx"
            // para así generar una nueva variable de proyección ya que
            // con ella se podrá diferenciar si los bindings que estén incluidos en
            // una subconsulta pertenecen a la consulta padre.
            projectionVars.add("__rowIdx");

            // Extrae la consulta proveniente del servicio asociada a las variables de proyección.
            String queryString = service.getQueryString(projectionVars);
            System.out.println("Las projection Vars son: " + projectionVars.toString());

            // Se declara una lista de bindings relevantes que contendrá todos aquellos bindings que indiquen
            // si las variables de proyección al final tendrán que ver una con la otra según allBindings
            // y las variables en el objeto service.
            List<String> relevantBindingNames = getRelevantBindingNames(allBindings, service.getServiceVars());

            if (relevantBindingNames.size() != 0) {
                // Ya que las variables de proyección pueden ser de tipo VALUE, se debe de construir la consulta
                // construyendo y agregando tal sección de consulta a la consulta total.
                queryString += buildVALUESClause(allBindings, relevantBindingNames);
            }

            // Al ser una consulta de tipo SELECT, se crea una variable que contenga la información asociada
            // a la consulta asociada a la consulta construida y a la URI del SPARQL endpoint remoto.
            TupleQuery query = getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString, baseUri);
            // Se crea una variable que guarde los resultados de la consulta (query).
            TupleQueryResult res = null;
            // Se intenta hacer la consulta
            try {
                // TODO obtener tiempo declarado desde la interfaz de Apache Marmotta.
                // Se declara el tiempo máximo de espera para la consulta (60 segundos).
                query.setMaxQueryTime(60);
                // Se ejecuta la consulta.
                res = query.evaluate();
            }
            // Si hubo un error, es porque se evaluo de manera incorrecta.
            catch (QueryEvaluationException q) {
                // Se desactiva la conexión al repositorio de datos.
                closeQuietly(res);

                // Ya que este método se ejecuta una vez por lote, cada vez que ocurre un error en el lote de
                // bindings asociado a dicho lote, se debe de registrar el resultado "vacío" indicando que ya se corrió
                // tal lote.
                // Esta tarea se logra con la clase ServiceFallBackIteration y usando el objeto service,los bindings y
                // el objeto que haga referencia al ejecutor de los método de la clase ServiceFallbackIteration.
                String preparedQuery = service.getQueryString(projectionVars);
                result = new ServiceFallbackIteration(service, preparedQuery, allBindings, this);
                // Si el la consulta está declarada como silenciosa, no se asigna el resultado de manera explícita
                // y si no, el resultado no se encapsula en un objeto.
                result = service.isSilent() ? new SilentIteration(result) : result;
                return result;
            }

            // Si lo anterior salío bien, se determina el número de variables libres.
            // Si las variables libres son cero, significa que, significa que no hay variables de proyección
            // que involucren variables presentes en una consulta padre en una consulta hija.
            //
            // Lo anterior involucra que toda variable que esté en la consulta padre se tiene que estar presente en
            // todos los resultados de la consulta hija. (Cross product)
            //
            // Pero si las variables relevantes existe, entonces se debe de hacer una unión entre resultados de
            // consulta padre y resultados de consulta hija (Common Join).
            //
            // Nota: Los métodos usados están alojados en OpenRDF
            //
            if (relevantBindingNames.size() == 0)
                result = new ServiceCrossProductIteration(res, allBindings);
            else
                result = new ServiceJoinConversionIteration(res, allBindings);

            // Si el la consulta está declarada como silenciosa, no se asigna el resultado de manera explícita
            // y si no, el resultado no se encapsula en un objeto.
            result = service.isSilent() ? new SilentIteration(result) : result;
            return result;
        }
        // Si hubo un error a la hora de evaluar la consulta asociada a algún lote,
        // se pueden sucitar las siguientes excepciones.
        // Excepción por conexión a repositorio de datos.
        catch (RepositoryException e) {
            Iterations.closeCloseable(result);
            if (service.isSilent())
                return new CollectionIteration<BindingSet, QueryEvaluationException>(allBindings);
            throw new QueryEvaluationException("SPARQLRepository for endpoint " + rep.toString()
                    + " could not be initialized.", e);
        }
        // Excepción por consulta mal formulada.
        catch (MalformedQueryException e) {
            // this exception must not be silenced, bug in our code
            throw new QueryEvaluationException(e);
        }
        // Excepción por evaluación de consulta.
        catch (QueryEvaluationException e) {
            Iterations.closeCloseable(result);
            if (service.isSilent())
                return new CollectionIteration<BindingSet, QueryEvaluationException>(allBindings);
            throw e;
        }
        // Excepción por un error en tiempo de ejecución.
        catch (RuntimeException e) {
            Iterations.closeCloseable(result);
            // suppress special exceptions (e.g. UndeclaredThrowable with wrapped
            // QueryEval) if silent
            if (service.isSilent())
                return new CollectionIteration<BindingSet, QueryEvaluationException>(allBindings);
            throw e;
        }
    }

    /**
    * Método encargado de inicializar la conexión al repositorio.
    * */
    public void initialize()
            throws RepositoryException {
        rep.initialize();
    }

    /**
    * Método encargado de cerrar la conexión asociado al objeto asociado a los resultados (res).
    * */
    private void closeQuietly(TupleQueryResult res) {
        // Intenta cerrar la conexión en caso de existir alguna.
        try {
            if (res != null)
                res.close();
        }
        // Si ocurre un error, registra en el archivo LOG indicando que no se pudo cerrar la conexión correctamente.
        catch (Exception e) {
            logger.debug("Could not close connection properly: " + e.getMessage(), e);
        }
    }

    public void shutdown()
            throws RepositoryException
    {
        if (conn != null)
            conn.close();
        rep.shutDown();
    }

    /**
    * Método que se encarga de establecer conexión al repositorio de datos mediante un cliente HTTP.
    * Retorna un objeto con la conexión
    * */
    protected RepositoryConnection getConnection()
            throws RepositoryException
    {
        // Si actualmente no existe un cliente HTTP para la conexión, establece uno.
        if (conn == null) {
            conn = rep.getConnection();
        }
        return conn;
    }

    /**
    * El siguiente método es el encargado de extraer lon bindings que necesitan ser proyectadas en subconsultas
    * usando las variable que tengan en común las variables del servicio y
    * la de los bindings que son suministrados como argumento de método.
    *
    * Regresa una lista de bindings en común que fueron determinados.
    * */
    private List<String> getRelevantBindingNames(List<BindingSet> bindings, Set<String> serviceVars) {
        // Recorre cada binding en "bindings" y agrega el binding a la lista de bindings relevantes
        // si el objeto "service" posee también dicho binding.
        List<String> relevantBindingNames = new ArrayList<String>(5);
        for (String bName : bindings.get(0).getBindingNames()) {
            if (serviceVars.contains(bName))
                relevantBindingNames.add(bName);
        }
        return relevantBindingNames;
    }

    /**
    * Con base a la síntaxis admitida por Apache Marmotta, las sentencias de consulta de tipo value son
    *
    * VALUES (?ejemplo) { ("ValorUno") ("ValorDos") }
    *
    * En el ejemplo de arriba se tiene que el binding ?ejemplo asume 2 valoes "ValorUno" y "ValorDos",
    * los valores están encerrados por dobles comillas por lo que en el procesamiento deben de ser \"
    * y delimitados por paréntesis del tipo "()" y el conjunto de valores que el binding ?ejemplo puede tener asignados
    * están delimitados por llaves "{}".
    *
    * VALUES (?relBindCero, ..., ?relBindN ) { ("1", ..., "ValN"), ..., ("1", ..., "ValN") }.
    *
    * */
    private String buildVALUESClause(List<BindingSet> bindings, List<String> relevantBindingNames)
            throws QueryEvaluationException
    {
        // Variable que contendrá la consulta construida.
        StringBuilder sb = new StringBuilder();

        // Ya que las variables que se consideran en este caso son de proyección, se debe usar la palabra "__rowIdx".
        // A continuación se inicializa la consulta que después será construida con la palabra reservada VALUES.
        sb.append(" VALUES (?__rowIdx"); // __rowIdx: see comment in evaluate()

        // Se continua con la construcción de la consulta VALUES agregando el delimitador de binding "?"
        // y la variable relevante.
        for (String bName : relevantBindingNames) {
            sb.append(" ?").append(bName);
        }

        // Cuando se termine de delimitar qué bindings son los involucrados, se usa el dilimitador "(" para indicar
        // que se terminó la sección de bindings y se usa el delimitador "{" para indicar que empieza la declaración
        // de valores asignables a los bindings relevantes.
        sb.append(") { ");

        // Retomando el comentario del identificador "rowIdx" de la clase evaluateInternal, se declara un contador
        // que indique qué fila se está trabajando.
        int rowIdx = 0;
        // Recorre cada binding y asigna sus valores
        for (BindingSet b : bindings) {
            // Empieza asginación de valores con "(". Luego se incrusta el valor de la fila correspondiente al binding.
            sb.append(" (");
            // Se termina de asignar el valor de la fila actualizado (rowIdx++).
            sb.append("\"").append(rowIdx++).append("\" ");

            // Se recorre cada binding relevante (relevantBindingNames) y se anexa a la consulta construida (sb)
            // el valor del binding en cuestión (b).
            for (String bName : relevantBindingNames) {
                // Convierte el valor en una cadena de caracteres. Luego se agrega como valor de la consulta construida
                appendValueAsString(sb, b.getValue(bName)).append(" ");
            }
            // Se termina la asginación del valor con el delimitador ")"
            sb.append(")");
        }
        // Se termina la asginación de todos los valores con el delimitador "}"
        sb.append(" }");
        return sb.toString();
    }

    /**
    * Método encargado de determiar qué tipo de valor es el argumento y luego castearlo a un String.
    * Se regresa un la consulta construida actualizada con el valor agregado.
    * */
    protected StringBuilder appendValueAsString(StringBuilder sb, Value value) {

        if (value == null)
            return sb.append("UNDEF");

        // Si el valor es de tipo URI, se usa el método appendURI para casterarlo a String.
        else if (value instanceof URI)
            return appendURI(sb, (URI)value);

        // Si el valor es de tipo Literal, se usa el método appendLiteral para casterarlo a String.
        else if (value instanceof Literal)
            return appendLiteral(sb, (Literal)value);

        // Si el valor no es reconocido por Apache Marmotta, se lanza una excepción mencionando que no es
        // posible identificar el tipo de dato del argumento.
        throw new RuntimeException("Type not supported: " + value.getClass().getCanonicalName());
    }

    /**
    * Método encargado de casterar la URI a un String válido
    * String válido: URL#nombreRecurso >> <URL#nombreRecurso>
    * */
    protected static StringBuilder appendURI(StringBuilder sb, URI uri) {
        sb.append("<").append(uri.stringValue()).append(">");
        return sb;
    }

    /**
    * Método encargado de castear a un String válido una literal.
    * String válido: Literal >> "Literal"^^<tipoDato>" o "Literal"@lenguaje.
    * */
    protected static StringBuilder appendLiteral(StringBuilder sb, Literal lit) {
        sb.append('"');
        sb.append(lit.getLabel().replace("\"", "\\\""));
        sb.append('"');

        // Si la literal corresponde a una literal de lenguaje, se agrega "@" y el lenguaje.
        if (lit.getLanguage() != null) {
            sb.append('@');
            sb.append(lit.getLanguage());
        }
        // Si corresponde a un tipo de dato en específico, se agrega ^^<tipoDato> a la consulta (sb).
        else if (lit.getDatatype() != null) {
            sb.append("^^<");
            sb.append(lit.getDatatype().stringValue());
            sb.append('>');
        }
        return sb;
    }
}
