package cloud.nimburst.tug;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class Tug {

    private final TugAction action;
    private final Path configRoot;
    private final TugManifest manifest;
    private final List<String> resources;
    private final int parallelism;

    public Tug(int parallelism, TugAction action, Path manifestPath, List<String> resources) {

        this.parallelism = parallelism;
        this.action = action;
        this.resources = resources;
        configRoot = manifestPath.toAbsolutePath().getParent();
        manifest = YamlParser.parseYaml(manifestPath, TugManifest.class, true);

        try {
            ApiClient client = Config.defaultClient();
            Configuration.setDefaultApiClient(client);
        } catch (IOException e) {
            throw new RuntimeException("Unable to configure k8s client", e);
        }
    }

    public void execute() {

        switch (action) {
            case PUSH:
                doAction(ResourceActionDirection.CREATE);
                break;
            case PULL:
                doAction(ResourceActionDirection.DELETE);
                break;
            case REPUSH:
                doAction(ResourceActionDirection.DELETE);
                doAction(ResourceActionDirection.CREATE);
                break;
            default:
                throw new RuntimeException("Invalid action: " + action);
        }
    }

    private void doAction(ResourceActionDirection dir) {

        if (dir == ResourceActionDirection.CREATE) {
            System.out.println("\uD83D\uDEA2 Pushing containers into port ... \uD83D\uDEA2");
        } else {
            System.out.println("\uD83D\uDEA2 Pulling containers out to sea ... \uD83D\uDEA2");
        }

        try {
            ResourceActionGraphExecutor
                    .build(dir, parallelism, configRoot, manifest, resources)
                    .execute();
            System.out.println("\uD83D\uDEA2 Toot Toot! \uD83D\uDEA2");
        } catch (Exception e) {
            System.out.println("A resource action failed. The cluster may be in an undesirable state. Manual intervention may be required.");
            throw new RuntimeException(e);
        }
    }
}
