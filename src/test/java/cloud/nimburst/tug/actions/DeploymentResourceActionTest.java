package cloud.nimburst.tug.actions;

import cloud.nimburst.tug.TugManifest;
import cloud.nimburst.tug.TugManifest.Deployment;
import cloud.nimburst.tug.YamlParser;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class DeploymentResourceActionTest {

    @Test
    public void validateOldVersion() {

        Path configRoot = Paths.get("src/test/resources").toAbsolutePath();

        Deployment deployment = new Deployment();
        deployment.setLocation("extensionsV1Beta1Deployment.yaml");

        Path location = Paths.get(deployment.getLocation());
        if (!location.isAbsolute()) {
            location = configRoot.resolve(location);
        }
        JsonNode resource = YamlParser.parseYaml(location);
        JsonNode kindNode = resource.get("kind");
        if(kindNode == null || !kindNode.isTextual()) {
            throw new RuntimeException("No kind defined in " + location);
        }


        DeploymentResourceAction action = new DeploymentResourceAction(resource, deployment);
    }
}