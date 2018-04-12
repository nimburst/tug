package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest;
import cloud.nimburst.tug.YamlParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.BatchV1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Job;
import io.kubernetes.client.models.V1JobList;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * {@link ResourceAction} for managing a Job resource
 */
public class JobResourceAction implements ResourceAction {

    private final String namespace;
    private final V1Job jobFile;
    private final BatchV1Api api = new BatchV1Api();
    private final int maxWaitSeconds;

    /**
     * Instantiates a new JobResourceAction.
     *
     * @param resource   the content of the yaml resource configuration
     * @param deployment the deployment configuration from the manifest
     */
    public JobResourceAction(JsonNode resource, TugManifest.Deployment deployment) {
        jobFile = YamlParser.transformYaml(resource, V1Job.class, false);
        String namespace = jobFile.getMetadata().getNamespace();
        this.namespace = namespace == null ? "default" : namespace;
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + jobFile.getMetadata().getName();
        V1JobList result;
        try {
            result = api.listNamespacedJob(namespace, null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Job info: " + e.getResponseBody());
        }
        return result.getItems().size() > 0;
    }

    private boolean resourceReady() throws ResourceActionException {

        V1Job job;
        try {
            job = api.readNamespacedJob(jobFile.getMetadata().getName(), namespace, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Job info: " + e.getResponseBody(), e);
        }

        Integer succeeded = job.getStatus().getSucceeded();
        return succeeded != null && succeeded > 0;
    }

    private void waitUntilReady() throws ResourceActionException {

        if (!resourceReady()) {
            System.out.println(String.format("waiting for Job '%s' to be ready", jobFile.getMetadata().getName()));
            Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

            pollWait();

            while (!resourceReady()) {
                if (Instant.now().isAfter(maxTime)) {
                    throw new ResourceActionException(String.format("Job '%s' was not ready in %d seconds", jobFile.getMetadata().getName(), maxWaitSeconds));
                }
                pollWait();
            }
        }
    }

    private void pollWait() {

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while checking Job status", e);
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

        System.out.println(String.format("creating Job '%s'", jobFile.getMetadata().getName()));
        try {
            api.createNamespacedJob(namespace, jobFile, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create Job: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for Job '%s' to be created", jobFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Job '%s' was not created in %d seconds", jobFile.getMetadata().getName(), maxWaitSeconds));
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

        System.out.println(String.format("waiting for Job '%s' to be deleted", jobFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Job '%s' was not deleted in %d seconds", jobFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting Job '%s'", jobFile.getMetadata().getName()));
        try {
            V1DeleteOptions deleteOptions = new V1DeleteOptions();
            deleteOptions.propagationPolicy("Foreground");
            api.deleteNamespacedJob(jobFile.getMetadata().getName(), namespace, deleteOptions, null, null, null, "Foreground");
        } catch (JsonSyntaxException e) {
            //https://github.com/kubernetes-client/java/issues/205
            //no-op
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete Job: " + e.getResponseBody(), e);
        }
    }
}
