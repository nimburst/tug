package cloud.nimburst.tug;

import cloud.nimburst.tug.TugManifest.Deployment;
import cloud.nimburst.tug.actions.ConfigMapResourceAction;
import cloud.nimburst.tug.actions.DeploymentResourceAction;
import cloud.nimburst.tug.actions.JobResourceAction;
import cloud.nimburst.tug.actions.PodResourceAction;
import cloud.nimburst.tug.actions.ServiceResourceAction;
import lombok.Data;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.nio.file.Path;
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

public class ResourceActionGraphExecutor {

    public static ResourceActionGraphExecutor build(ResourceActionDirection direction, int parallelism, Path configRoot, TugManifest manifest, List<String> resources) {

        return new ResourceActionGraphExecutor(direction, parallelism, configRoot, manifest, resources);
    }

    private final DirectedAcyclicGraph<DeploymentAction, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    private final Set<DeploymentAction> processing = new HashSet<>();
    final Set<DeploymentAction> initial;
    final Map<String, DeploymentAction> deploymentActions;
    final List<CompletableFuture<?>> futures;
    private final ExecutorService executor;
    private final Path configRoot;
    private final String namespace;
    private final ResourceActionDirection dir;
    private Exception exception;
    private CompletableFuture<?> all;

    private ResourceActionGraphExecutor(ResourceActionDirection direction, int parallelism, Path configRoot, TugManifest manifest, List<String> resources) {

        this.dir = direction;
        this.namespace = manifest.getNamespace();
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

        ResourceAction resourceAction;
        switch (deployment.getKind()) {
            case Pod:
                resourceAction = new PodResourceAction(namespace, configRoot, deployment);
                break;
            case Service:
                resourceAction = new ServiceResourceAction(namespace, configRoot, deployment);
                break;
            case ConfigMap:
                resourceAction = new ConfigMapResourceAction(namespace, configRoot, deployment);
                break;
            case Job:
                resourceAction = new JobResourceAction(namespace, configRoot, deployment);
                break;
            case Deployment:
                resourceAction = new DeploymentResourceAction(namespace, configRoot, deployment);
                break;
            default:
                throw new RuntimeException("Unsupported deployment kind: " + deployment.getKind());
        }
        return new DeploymentAction(deployment, resourceAction);
    }

    public void execute() throws Exception {

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

    @Data
    private static class DeploymentAction {

        private final Deployment deployment;
        private final ResourceAction resourceAction;
        private final CompletableFuture<DeploymentAction> future;

        public DeploymentAction(Deployment deployment, ResourceAction resourceAction) {

            this.deployment = deployment;
            this.resourceAction = resourceAction;
            this.future = new CompletableFuture<>();
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
