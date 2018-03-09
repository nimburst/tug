package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest;
import cloud.nimburst.tug.YamlParser;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapList;
import io.kubernetes.client.models.V1DeleteOptions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class ConfigMapResourceAction implements ResourceAction {

    private final String namespace;
    private final V1ConfigMap configMapFile;
    private final CoreV1Api api = new CoreV1Api();
    private final int maxWaitSeconds;

    public ConfigMapResourceAction(String namespace, Path configRoot, TugManifest.Deployment deployment) {

        this.namespace = namespace;
        Path location = Paths.get(deployment.getLocation());
        if (!location.isAbsolute()) {
            location = configRoot.resolve(location);
        }
        configMapFile = YamlParser.parseYaml(location, V1ConfigMap.class, false);
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + configMapFile.getMetadata().getName();
        V1ConfigMapList result;
        try {
            result = api.listNamespacedConfigMap(namespace, null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get ConfigMap info: " + e.getResponseBody());
        }
        return result.getItems().size() > 0;
    }

    @Override
    public void makeReady() throws ResourceActionException {

        if (!resourceExists()) {
            create();
            waitUntilCreated();
        }
    }

    private void create() throws ResourceActionException {
        System.out.println(String.format("creating ConfigMap '%s'", configMapFile.getMetadata().getName()));
        try {
            api.createNamespacedConfigMap(namespace, configMapFile, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create ConfigMap: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for ConfigMap '%s' to be created", configMapFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("ConfigMap '%s' was not created in %d seconds", configMapFile.getMetadata().getName(), maxWaitSeconds));
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

        System.out.println(String.format("waiting for ConfigMap '%s' to be deleted", configMapFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("ConfigMap '%s' was not deleted in %d seconds", configMapFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting ConfigMap '%s'", configMapFile.getMetadata().getName()));
        try {
            V1DeleteOptions deleteOptions = new V1DeleteOptions();
            deleteOptions.propagationPolicy("Foreground");
            api.deleteNamespacedConfigMap(configMapFile.getMetadata().getName(), namespace, deleteOptions, null, null, null, "Foreground");
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete ConfigMap: " + e.getResponseBody(), e);
        }
    }

    private void pollWait() {

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while checking ConfigMap status", e);
        }
    }
}
