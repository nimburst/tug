package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest;
import cloud.nimburst.tug.YamlParser;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServiceList;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class ServiceResourceAction implements ResourceAction {

    private final String namespace;
    private final V1Service serviceFile;
    private final CoreV1Api api = new CoreV1Api();
    private final int maxWaitSeconds;

    public ServiceResourceAction(String namespace, Path configRoot, TugManifest.Deployment deployment) {

        this.namespace = namespace;
        Path location = Paths.get(deployment.getLocation());
        if (!location.isAbsolute()) {
            location = configRoot.resolve(location);
        }
        serviceFile = YamlParser.parseYaml(location, V1Service.class, false);
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + serviceFile.getMetadata().getName();
        V1ServiceList result;
        try {
            result = api.listNamespacedService(namespace, null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Service info: " + e.getResponseBody());
        }
        return result.getItems().size() > 0;
    }

    @Override
    public void makeReady() throws ResourceActionException {

        if (!resourceExists()) {
            create();
            waitUntilCreated();
            //wait a second longer to ensure service is ready to route requests
            pollWait();
        }
    }

    private void create() throws ResourceActionException {

        System.out.println(String.format("creating Service '%s'", serviceFile.getMetadata().getName()));
        try {
            api.createNamespacedService(namespace, serviceFile, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create Service: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for Service '%s' to be created", serviceFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Service '%s' was not created in %d seconds", serviceFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }


    @Override
    public void delete() throws ResourceActionException {

        if (resourceExists()) {
            executeDelete();
            waitUntilDeleted();
        }
    }

    private void waitUntilDeleted() throws ResourceActionException {

        System.out.println(String.format("waiting for Service '%s' to be deleted", serviceFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Service '%s' was not deleted in %d seconds", serviceFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting Service '%s'", serviceFile.getMetadata().getName()));
        try {
            api.deleteNamespacedService(serviceFile.getMetadata().getName(), namespace, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete Service: " + e.getResponseBody(), e);
        }
    }

    private void pollWait() {

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while checking Service status", e);
        }
    }
}
