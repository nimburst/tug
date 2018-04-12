package cloud.nimburst.tug;

import cloud.nimburst.tug.TugManifest.Deployment;
import cloud.nimburst.tug.actions.ClusterRoleBindingAction;
import cloud.nimburst.tug.actions.ConfigMapResourceAction;
import cloud.nimburst.tug.actions.DeploymentResourceAction;
import cloud.nimburst.tug.actions.IngressResourceAction;
import cloud.nimburst.tug.actions.JobResourceAction;
import cloud.nimburst.tug.actions.NamespaceResourceAction;
import cloud.nimburst.tug.actions.PodResourceAction;
import cloud.nimburst.tug.actions.ServiceResourceAction;
import com.fasterxml.jackson.databind.JsonNode;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executes {@link ResourceAction}s.
 */
public class ResourceActionGraphExecutor {

    private final DirectedAcyclicGraph<DeploymentAction, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    private final Set<DeploymentAction> processing = new HashSet<>();
    private final Set<DeploymentAction> initial;
    private final Map<String, DeploymentAction> deploymentActions;
    private final List<CompletableFuture<?>> futures;
    private final ExecutorService executor;
    private final Path configRoot;
    private final ResourceActionDirection dir;
    private Exception exception;
    private CompletableFuture<?> all;

    /**
     * Instantiates a new ResourceActionGraphExecutors.
     *
     * @param direction   create or delete
     * @param parallelism max number of concurrent actions
     * @param configRoot  the directory containing the manifest
     * @param manifest    the manifest
     * @param resources   the resources specified on the command line or an empty list for all defined in the manifest
     */
    public ResourceActionGraphExecutor(ResourceActionDirection direction, int parallelism, Path configRoot, TugManifest manifest, List<String> resources) {

        this.dir = direction;
        this.configRoot = configRoot;
        executor = Executors.newWorkStealingPool(parallelism);
        deploymentActions = manifest.getDeployments().stream()
                .map(this::deploymentToAction)
                .collect(Collectors.toMap(da -> da.getDeployment().getName(), Function.identity()));

        deploymentActions.values().forEach(dag::addVertex);
        deploymentActions.values().forEach(
                da -> da.getDeployment().getDependencies().forEach(dep -> dag.addEdge(da, deploymentActions.get(dep))));

        if (!resources.isEmpty()) {
            Set<DeploymentAction> toKeep = resources.stream()
                    .map(deploymentActions::get)
                    .flatMap(da -> (dir == ResourceActionDirection.CREATE ? dag.getDescendants(da) : dag.getAncestors(da)).stream())
                    .collect(Collectors.toCollection(HashSet::new));
            toKeep.addAll(resources.stream()
                    .map(deploymentActions::get)
                    .collect(Collectors.toCollection(HashSet::new)));

            futures = toKeep.stream().map(DeploymentAction::getFuture).collect(Collectors.toList());

            Set<DeploymentAction> toRemove = new HashSet<>(deploymentActions.values());
            toRemove.removeAll(toKeep);

            toRemove.forEach(dag::removeVertex);
        } else {
            futures = deploymentActions.values().stream().map(DeploymentAction::getFuture).collect(Collectors.toList());
        }

        initial = new HashSet<>();
        dag.forEach(v -> {
            if ((dir == ResourceActionDirection.CREATE ? dag.outgoingEdgesOf(v) : dag.incomingEdgesOf(v)).isEmpty()) {
                processing.add(v);
                initial.add(v);
            }
        });
    }

    private DeploymentAction deploymentToAction(Deployment deployment) {

        Path location = Paths.get(deployment.getLocation());
        if (!location.isAbsolute()) {
            location = configRoot.resolve(location);
        }
        JsonNode resource = YamlParser.parseYaml(location);
        JsonNode kindNode = resource.get("kind");
        if(kindNode == null || !kindNode.isTextual()) {
            throw new RuntimeException("No kind defined in " + location);
        }

        String kind = kindNode.textValue();

        ResourceAction resourceAction;
        switch (kind) {
            case "Pod":
                resourceAction = new PodResourceAction(resource, deployment);
                break;
            case "Service":
                resourceAction = new ServiceResourceAction(resource, deployment);
                break;
            case "ConfigMap":
                resourceAction = new ConfigMapResourceAction(resource, deployment);
                break;
            case "Job":
                resourceAction = new JobResourceAction(resource, deployment);
                break;
            case "Deployment":
                resourceAction = new DeploymentResourceAction(resource, deployment);
                break;
            case "Ingress":
                resourceAction = new IngressResourceAction(resource, deployment);
                break;
            case "Namespace":
                resourceAction = new NamespaceResourceAction(resource, deployment);
                break;
            case "ClusterRoleBinding":
                resourceAction = new ClusterRoleBindingAction(resource, deployment);
                break;
            default:
                throw new RuntimeException("Unsupported deployment kind: " + kind);
        }
        return new DeploymentAction(deployment, resourceAction);
    }

    /**
     * Executes the resource actions.
     *
     */
    public void execute() {

        try {
            initial.forEach(v -> executor.submit(() -> this.doNext(v)));
            all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
            all.join();
        } finally {
            executor.shutdownNow();
            try {
                executor.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                //no-op
            }
        }
    }

    private Void doNext(DeploymentAction v) {

        if (exception != null) {
            v.getFuture().completeExceptionally(exception);
        }


        try {
            if (dir == ResourceActionDirection.CREATE) {
                v.getResourceAction().makeReady();
                System.out.println(v.getDeployment().getName() + " ready");
            } else {
                v.getResourceAction().delete();
                System.out.println(v.getDeployment().getName() + " deleted");
            }
            v.getFuture().complete(v);
            synchronized (dag) {
                dag.removeVertex(v);
                dag.forEach(nextV -> {
                    if (!processing.contains(nextV) && (dir == ResourceActionDirection.CREATE ? dag.outgoingEdgesOf(nextV) : dag.incomingEdgesOf(nextV)).isEmpty()) {
                        processing.add(nextV);
                        executor.submit(() -> this.doNext(nextV));
                    }
                });
            }
        } catch (Exception e) {
            exception = e;
            v.getFuture().completeExceptionally(e);
            all.completeExceptionally(e);
        }
        return null;
    }

    private static class DeploymentAction {

        private final Deployment deployment;
        private final ResourceAction resourceAction;
        private final CompletableFuture<DeploymentAction> future;

        private DeploymentAction(Deployment deployment, ResourceAction resourceAction) {

            this.deployment = deployment;
            this.resourceAction = resourceAction;
            this.future = new CompletableFuture<>();
        }

        private Deployment getDeployment() {
            return deployment;
        }

        private ResourceAction getResourceAction() {
            return resourceAction;
        }

        private CompletableFuture<DeploymentAction> getFuture() {
            return future;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            DeploymentAction that = (DeploymentAction) o;
            return Objects.equals(deployment.getName(), that.deployment.getName());
        }

        @Override
        public int hashCode() {

            return deployment.getName().hashCode();
        }
    }
}
