package org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Service;
import org.openrdf.repository.RepositoryException;

public interface FederatedService {

    /**
     * El tipo de consultas que se admiten son:
     */
    public static enum QueryType {
        SELECT,
        ASK
    }

    // Al ser el actual archivo una interfaz, solo se muestran las firmas de los 2 métodos posibles
    // a usar en una consulta federada.

    /**
    * Con base a la sesión del SPARQL endpoint remoto (service)
    * asociado a la URI pasada como argumento (baseUri),
    * se evalua la consulta (sparqlQueryString)
    * según el tipo de consulta (type)
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
                                                                             FederatedService.QueryType type,
                                                                             Service service)
            throws QueryEvaluationException;



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
     * */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
                                                                             CloseableIteration<BindingSet, QueryEvaluationException> bindings,
                                                                             String baseUri)
            throws QueryEvaluationException;

    /**
     * Método que inicializa la conexión con el repositorio de datos.
     */
    public void initialize()
            throws RepositoryException;

    /**
    * Método que deshabilita las conexiones a repositorios de datos.
    * */
    public void shutdown()
            throws RepositoryException;

}
