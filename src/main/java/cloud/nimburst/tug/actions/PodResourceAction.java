package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.ResourceActionException;
import cloud.nimburst.tug.TugManifest;
import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ContainerStatus;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import cloud.nimburst.tug.ResourceAction;
import cloud.nimburst.tug.YamlParser;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class PodResourceAction implements ResourceAction {

    private final String namespace;
    private final V1Pod podFile;
    private final CoreV1Api api = new CoreV1Api();
    private final int maxWaitSeconds;

    public PodResourceAction(String namespace, Path configRoot, TugManifest.Deployment deployment) {

        this.namespace = namespace;
        Path location = Paths.get(deployment.getLocation());
        if (!location.isAbsolute()) {
            location = configRoot.resolve(location);
        }
        podFile = YamlParser.parseYaml(location, V1Pod.class, false);
        this.maxWaitSeconds = deployment.getMaxWaitSeconds();
    }

    private boolean resourceExists() throws ResourceActionException {

        String selector = "metadata.name=" + podFile.getMetadata().getName();
        V1PodList result;
        try {
            result = api.listNamespacedPod(namespace, null, null, selector, true, null, null, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Pod info: " + e.getResponseBody());
        }
        return result.getItems().size() > 0;
    }

    private boolean resourceReady() throws ResourceActionException {

        V1Pod pod;
        try {
            pod = api.readNamespacedPod(podFile.getMetadata().getName(), namespace, null, null, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to get Pod info: " + e.getResponseBody(), e);
        }
        List<V1ContainerStatus> statuses = pod.getStatus().getContainerStatuses();
        if(statuses == null) {
            return false;
        }
        return statuses.stream().allMatch(status -> status != null && status.isReady());
    }

    private void waitUntilReady() throws ResourceActionException {

        if (!resourceReady()) {
            System.out.println(String.format("waiting for Pod '%s' to be ready", podFile.getMetadata().getName()));
            Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

            pollWait();

            while (!resourceReady()) {
                if (Instant.now().isAfter(maxTime)) {
                    throw new ResourceActionException(String.format("Pod '%s' was not ready in %d seconds", podFile.getMetadata().getName(), maxWaitSeconds));
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
            throw new RuntimeException("Thread was interrupted while checking Pod status", e);
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

        System.out.println(String.format("creating Pod '%s'", podFile.getMetadata().getName()));
        try {
            api.createNamespacedPod(namespace, podFile, null);
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to create Pod: " + e.getResponseBody(), e);
        }
    }

    private void waitUntilCreated() throws ResourceActionException {

        System.out.println(String.format("waiting for Pod '%s' to be created", podFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (!resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Pod '%s' was not created in %d seconds", podFile.getMetadata().getName(), maxWaitSeconds));
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

        System.out.println(String.format("waiting for Pod '%s' to be deleted", podFile.getMetadata().getName()));
        Instant maxTime = Instant.now().plus(maxWaitSeconds, ChronoUnit.SECONDS);

        pollWait();

        while (resourceExists()) {
            if (Instant.now().isAfter(maxTime)) {
                throw new ResourceActionException(String.format("Pod '%s' was not deleted in %d seconds", podFile.getMetadata().getName(), maxWaitSeconds));
            }
            pollWait();
        }
    }

    private void executeDelete() throws ResourceActionException {

        System.out.println(String.format("deleting Pod '%s'", podFile.getMetadata().getName()));
        try {
            V1DeleteOptions deleteOptions = new V1DeleteOptions();
            deleteOptions.propagationPolicy("Foreground");
            api.deleteNamespacedPod(podFile.getMetadata().getName(), namespace, deleteOptions, null, null, null, "Foreground");
        } catch (JsonSyntaxException e) {
            //https://github.com/kubernetes-client/java/issues/205
            //no-op
        } catch (ApiException e) {
            throw new ResourceActionException("Unable to delete Pod: " + e.getResponseBody(), e);
        }
    }
}
