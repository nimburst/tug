package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest.Deployment;
import cloud.nimburst.tug.YamlParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1beta1Ingress;
import io.kubernetes.client.models.V1beta1IngressList;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * {@link ResourceAction} for managing an Ingress resource
 */
public class IngressResourceAction implements ResourceAction {

    private final String namespace;
    private final V1beta1Ingress ingressFile;
    private final ExtensionsV1beta1Api api = new ExtensionsV1beta1Api();
    private final int maxWaitSeconds;

    /**
     * Instantiates a new IngressResourceAction.
     *
     * @param resource   the content of the yaml resource configuration
     * @param deployment the deployment configuration from the manifest
     */
    public IngressResourceAction(JsonNode resource, Deployment deployment) {
        ingressFile = YamlParser.transformYaml(resource, V1beta1Ingress.class, false);
        String namespace = ingressFile.getMetadata().getNamespace();
        this.namespace = namespace == null ? "default" : namespace;
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + ingressFile.getMetadata().getName();
        V1beta1IngressList result;
        try {
            result = api.listNamespacedIngress(namespace, null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Ingress info: " + e.getResponseBody());
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
        System.out.println(String.format("creating Ingress '%s'", ingressFile.getMetadata().getName()));
        try {
            api.createNamespacedIngress(namespace, ingressFile, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create Ingress: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for Ingress '%s' to be created", ingressFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Ingress '%s' was not created in %d seconds", ingressFile.getMetadata().getName(), maxWaitSeconds));
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

        System.out.println(String.format("waiting for Ingress '%s' to be deleted", ingressFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Ingress '%s' was not deleted in %d seconds", ingressFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting Ingress '%s'", ingressFile.getMetadata().getName()));
        try {
            V1DeleteOptions deleteOptions = new V1DeleteOptions();
            deleteOptions.propagationPolicy("Foreground");
            api.deleteNamespacedIngress(ingressFile.getMetadata().getName(), namespace, deleteOptions, null, null, null, "Foreground");
        } catch (JsonSyntaxException e) {
            //https://github.com/kubernetes-client/java/issues/205
            //no-op
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete Ingress: " + e.getResponseBody(), e);
        }
    }

    private void pollWait() {

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while checking Ingress status", e);
        }
    }
}
