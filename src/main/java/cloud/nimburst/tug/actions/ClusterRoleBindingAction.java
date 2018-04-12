package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest;
import cloud.nimburst.tug.YamlParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.models.V1ClusterRoleBinding;
import io.kubernetes.client.models.V1ClusterRoleBindingList;
import io.kubernetes.client.models.V1DeleteOptions;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * {@link ResourceAction} for managing a ClusterRoleBinding resource
 */
public class ClusterRoleBindingAction implements ResourceAction {

    private final V1ClusterRoleBinding clusterRoleBinding;
    private final RbacAuthorizationV1Api api = new RbacAuthorizationV1Api();
    private final int maxWaitSeconds;

    /**
     * Instantiates a new ClusterRoleBindingAction.
     *
     * @param resource   the content of the yaml resource configuration
     * @param deployment the deployment configuration from the manifest
     */
    public ClusterRoleBindingAction(JsonNode resource, TugManifest.Deployment deployment) {
        this.clusterRoleBinding = YamlParser.transformYaml(resource, V1ClusterRoleBinding.class, false);
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + clusterRoleBinding.getMetadata().getName();
        V1ClusterRoleBindingList result;
        try {
            result = api.listClusterRoleBinding(null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get ClusterRoleBinding info: " + e.getResponseBody());
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
        System.out.println(String.format("creating ClusterRoleBinding '%s'", clusterRoleBinding.getMetadata().getName()));
        try {
            api.createClusterRoleBinding(clusterRoleBinding, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create ClusterRoleBinding: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for ClusterRoleBinding '%s' to be created", clusterRoleBinding.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("ClusterRoleBinding '%s' was not created in %d seconds", clusterRoleBinding.getMetadata().getName(), maxWaitSeconds));
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

        System.out.println(String.format("waiting for ClusterRoleBinding '%s' to be deleted", clusterRoleBinding.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("ClusterRoleBinding '%s' was not deleted in %d seconds", clusterRoleBinding.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting ClusterRoleBinding '%s'", clusterRoleBinding.getMetadata().getName()));
        try {
            V1DeleteOptions deleteOptions = new V1DeleteOptions();
            deleteOptions.propagationPolicy("Foreground");
            api.deleteClusterRoleBinding(clusterRoleBinding.getMetadata().getName(), deleteOptions, null, null, null, "Foreground");
        } catch (JsonSyntaxException e) {
            //https://github.com/kubernetes-client/java/issues/205
            //no-op
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete ClusterRoleBinding: " + e.getResponseBody(), e);
        }
    }

    private void pollWait() {

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while checking ClusterRoleBinding status", e);
        }
    }
}