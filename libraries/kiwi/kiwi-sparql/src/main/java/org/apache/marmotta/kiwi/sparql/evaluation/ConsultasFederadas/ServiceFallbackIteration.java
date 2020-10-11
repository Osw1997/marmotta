package org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas;

import java.util.Collection;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.SingletonIteration;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.evaluation.federation.JoinExecutorBase;
import org.openrdf.query.algebra.evaluation.iterator.SilentIteration;
import org.openrdf.query.impl.EmptyBindingSet;


/**
 * Clase encargada de administrar los errores ocurridos en la evaluación de consulta por lotes en
 * SPARQLFederatedService
 * */
public class ServiceFallbackIteration extends JoinExecutorBase<BindingSet> {

    // Variable que cuyo valor solo puede modificarse una vez,
    // en este caso es la referencia a la sesión de la conexión al SPARQL endpoint remoto.
    protected final Service service;

    // Variable que almacenará la consulta construida asociada al lote que se esté evaluando (evaluateInternal)
    protected final String preparedQuery;

    // Se declara la interfaz por implementar, en este caso es federatedService.
    protected final org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.FederatedService federatedService;

    // Variable que guarda los bindings de la consulta.
    protected final Collection<BindingSet> bindings;

    // Constructor de la clase
    public ServiceFallbackIteration(Service service, String preparedQuery, Collection<BindingSet> bindings,
                                    FederatedService federatedService)
            throws QueryEvaluationException
    {
        super(null, null, EmptyBindingSet.getInstance());
        this.service = service;
        this.preparedQuery = preparedQuery;
        this.bindings = bindings;
        this.federatedService = federatedService;
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

    @Override
    protected void handleBindings()
            throws Exception
    {
        // Procesa cada binding y lo agrega al resultado
        for (BindingSet b : bindings) {
            try {
                CloseableIteration<BindingSet, QueryEvaluationException> result = federatedService.evaluate(
                        preparedQuery, b, service.getBaseURI(), FederatedService.QueryType.SELECT, service);
                // Si la sesion de la conexión con el SPARQL endpoint remoto es silenciosa,
                // no se devuelve el resultado de una manera explícita. De otra forma, se agrega el resultado.
                result = service.isSilent() ? new SilentIteration(result) : result;
                addResult(result);
            }
            // si ocurrió un error en la evaluación de la consulta y no es silenciosa la sesión, se lanza un error 'e',
            // de otra manera, se agrega un objeto vacío.
            catch (QueryEvaluationException e) {
                if (service.isSilent()) {
                    addResult(new SingletonIteration<BindingSet, QueryEvaluationException>(b));
                } else {
                    throw e;
                }
            }
            // si ocurrió un error en el tiempo de ejecución de la consulta y no es silenciosa la sesión ...
            catch (RuntimeException e) {
                if (service.isSilent()) {
                    addResult(new SingletonIteration<BindingSet, QueryEvaluationException>(b));
                }
                else {
                    throw e;
                }
            }
        }

    }


}