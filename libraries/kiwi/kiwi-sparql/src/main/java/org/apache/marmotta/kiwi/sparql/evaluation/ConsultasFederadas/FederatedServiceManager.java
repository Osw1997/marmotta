package org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas;

import java.util.concurrent.ConcurrentHashMap;

import org.openrdf.repository.RepositoryException;

/**
* Clase encargada de administrar las conexiones a SPARQL endpoints remotos.
* */
public class FederatedServiceManager {

    // Se declara una clase anindada que sirve para la instancia que se llegue a crear en una consulta federada.
    private static Class<? extends org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.FederatedServiceManager> implementationClass = org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.FederatedServiceManager.class;

    // Se iniciaiza la instancia con un valor nulo.
    private static volatile org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.FederatedServiceManager instance = null;

    // Método que regresa una clase inicializada a quien lo requiera (KiWiEvaluationStrategy).
    public static org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.FederatedServiceManager getInstance() {
        if (instance == null) {
            try {
                instance = implementationClass.newInstance();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return instance;
    }

    // Probar si quitandola, continua el funcionamiento
    public static synchronized void setImplementationClass(
            Class<? extends org.apache.marmotta.kiwi.sparql.evaluation.ConsultasFederadas.FederatedServiceManager> implementationClass)
    {
        FederatedServiceManager.implementationClass = implementationClass;
        try {
            instance = implementationClass.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Constructor vacío del método.
    public FederatedServiceManager() {
        ;
    }

    /**
     * Variable que servirá para asociar la URL del servicio al objeto FederatedService inicializado.
     */
    private ConcurrentHashMap<String, FederatedService> endpointToService = new ConcurrentHashMap<String, FederatedService>();

    /**
    * Método encargdo de asignar la URL del servicio al objeto FederatedService inicializado.
    * */
    public void registerService(String serviceUrl, FederatedService service) {
        endpointToService.put(serviceUrl, service);
    }

    /**
    * Método encargado de desenlazar la URL del servicio del objeto FederatedService.
    * */
    public void unregisterService(String serviceUrl) {
        FederatedService service = endpointToService.remove(serviceUrl);
        // Si existe sesión de servicio, deshabilita el servicio.
        if (service != null) {
            try {
                service.shutdown();
            }
            catch (RepositoryException e) {
                // TODO issue a warning, otherwise ignore
            }
        }
    }

    /**
    * Método encargado de devolver un objeto FederatedService asociado a la URL proporcionada.
    * Si no existe servicio asociado a la URL, crea una nueva instancia que esté asociada a la URL y
    * luego se registra como tupla en la variable endpointToService.
    * */
    public FederatedService getService(String serviceUrl)
            throws RepositoryException
    {
        FederatedService service = endpointToService.get(serviceUrl);
        if (service == null) {
            service = new SPARQLFederatedService(serviceUrl);
            // Inicializa la conexión al repositorio (clase SPARQLFederatedService)
            service.initialize();
            endpointToService.put(serviceUrl, service);
        }
        return service;
    }

    /**
    * Método encargado de deshabilitar todas las sesiones de SPARQL enpoint remotos activos siempre y cuando
    * la conexión esté siendo usada. Si así lo fuera,
    * se espera hasta que esté libre el recurso (synchronized(endpointToService)).
    * Finalmente, se borra toda la información que esté en caché de la variable endpointToService.
    * */
    public void unregisterAll() {
        synchronized (endpointToService) {
            for (FederatedService service : endpointToService.values()) {
                try {
                    service.shutdown();
                }
                catch (RepositoryException e) {
                    // TODO issue a warning, otherwise ignore
                }
            }
            endpointToService.clear();
        }
    }


}

