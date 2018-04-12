package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest;
import cloud.nimburst.tug.YamlParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1beta2Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1beta2Deployment;
import io.kubernetes.client.models.V1beta2DeploymentList;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * {@link ResourceAction} for managing a Deployment resource
 */
public class DeploymentResourceAction implements ResourceAction {

    private final String namespace;
    private final V1beta2Deployment deploymentFile;
    private final AppsV1beta2Api api = new AppsV1beta2Api();
    private final int maxWaitSeconds;

    /**
     * Instantiates a new DeploymentResourceAction.
     *
     * @param resource   the content of the yaml resource configuration
     * @param deployment the deployment configuration from the manifest
     */
    public DeploymentResourceAction(JsonNode resource, TugManifest.Deployment deployment) {
        deploymentFile = YamlParser.transformYaml(resource, V1beta2Deployment.class, false);
        String namespace = deploymentFile.getMetadata().getNamespace();
        this.namespace = namespace == null ? "default" : namespace;
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + deploymentFile.getMetadata().getName();
        V1beta2DeploymentList result;
        try {
            result = api.listNamespacedDeployment(namespace, null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Deployment info: " + e.getResponseBody());
        }
        return result.getItems().size() > 0;
    }

    private boolean resourceReady() throws ResourceActionException {

        V1beta2Deployment deployment;
        try {
            deployment = api.readNamespacedDeployment(deploymentFile.getMetadata().getName(), namespace, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Deployment info: " + e.getResponseBody(), e);
        }
        Integer replicas = deployment.getStatus().getAvailableReplicas();
        return replicas != null && replicas > 0;
    }

    private void waitUntilReady() throws ResourceActionException {

        if(!resourceReady()) {
            System.out.println(String.format("waiting for Deployment '%s' to be ready", deploymentFile.getMetadata().getName()));
            Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

            pollWait();

            while (!resourceReady()) {
                if (Instant.now().isAfter(maxTime)) {
                    throw new ResourceActionException(String.format("Deployment '%s' was not ready in %d seconds", deploymentFile.getMetadata().getName(), maxWaitSeconds));
                }
                pollWait();
            }
        }
    }

    private void pollWait(){
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while checking Deployment status", e);
        }
    }

    @Override
    public void makeReady() throws ResourceActionException {

        if (!resourceExists()) {
            create();
            waitUntilCreated();
        }
        waitUntilReady();
    }

    private void create() throws ResourceActionException {
        System.out.println(String.format("creating Deployment '%s'", deploymentFile.getMetadata().getName()));
        try {
            api.createNamespacedDeployment(namespace, deploymentFile, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create Deployment: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for Deployment '%s' to be created", deploymentFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Deployment '%s' was not created in %d seconds", deploymentFile.getMetadata().getName(), maxWaitSeconds));
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

        System.out.println(String.format("waiting for Deployment '%s' to be deleted", deploymentFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Deployment '%s' was not deleted in %d seconds", deploymentFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting Deployment '%s'", deploymentFile.getMetadata().getName()));
        try {
            V1DeleteOptions deleteOptions = new V1DeleteOptions();
            deleteOptions.propagationPolicy("Foreground");
            api.deleteNamespacedDeployment(deploymentFile.getMetadata().getName(), namespace, deleteOptions, null, null, null, "Foreground");
        } catch (JsonSyntaxException e) {
            //https://github.com/kubernetes-client/java/issues/205
            //no-op
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete Deployment: " + e.getResponseBody(), e);
        }
    }
}
