package cloud.nimburst.tug;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

//TODO validate dependencies
//TODO validate names are unique

@UniqueNames
public class TugManifest {
    @Valid
    private List<Deployment> deployments = new LinkedList<>();

    public List<Deployment> getDeployments() {
        return deployments;
    }

    public void setDeployments(List<Deployment> deployments) {
        this.deployments = deployments;
    }

    //TODO validate file exists
    public static class Deployment {
        @NotBlank
        private String name;
        @Min(1)
        private int maxWaitSeconds = 300;
        @NotBlank
        private String location;
        private Set<String> dependencies = new HashSet<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getMaxWaitSeconds() {
            return maxWaitSeconds;
        }

        public void setMaxWaitSeconds(int maxWaitSeconds) {
            this.maxWaitSeconds = maxWaitSeconds;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public Set<String> getDependencies() {
            return dependencies;
        }

        public void setDependencies(Set<String> dependencies) {
            this.dependencies = dependencies;
        }
    }
}
