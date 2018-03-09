package cloud.nimburst.tug;

import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

//TODO validate dependencies
//TODO validate names are unique

@UniqueNames
@Data
public class TugManifest
{
    @NotBlank
    private String namespace = "default";
    @Valid
    private List<Deployment> deployments = new LinkedList<>();

    //TODO validate file exists
    @Data
    public static class Deployment
    {
        @NotBlank
        private String name;
        @Min(1)
        private int maxWaitSeconds = 300;
        @NotNull
        private DeploymentKind kind;
        @NotBlank
        private String location;
        private Set<String> dependencies = new HashSet<>();
    }
}
