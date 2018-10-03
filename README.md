# Tug

Tug is a simple tool used to manage deployments to a Kubernetes cluster.  It is dependency and readiness aware to ensure
that resources are deployed in the correct order and are running.

Tug currently supports the following resource types:
* Pod
* Service
* ConfigMap
* Job
* Deployment
* Ingress
* Namespace
* ClusterRoleBinding

## Usage

\* This project is still in incubation.  ... more details to come ...

### Prerequisites 

TODO

### Manifest
Tug defines the deployment of resources in a manifest YAML file.  This file describes the location of Kubernetes 
configuration and the dependencies between them.

```
deployments:
  - name: effectPod
    location: foo/bar/effect-pod.yaml
    dependencies:
      - causePod
  - name: causePod
    location: foo/bar/cause-pod.yaml
    maxWaitSeconds: 300
```

By default tug will look in the local directory for a file named "tug-manifest.yaml".  This file can also be specified 
with the -m or --manifest options.  If the location of a Kubernetes configuration file specified in the manifest file is
 a relative path, it is relative to the manifest file.
 
The file consists of a list Kubernetes configuration files, specified by the location parameter.  Each configuration is 
given a name that is used on the command line and in dependency definitions.

For a given configuration, a list of dependencies can be specified.  This configuration will not be deployed until the
specified dependencies have been started and are healthy.
 
The maxWaitSeconds parameter is optional and defaults to 300 seconds if omitted.


### Running Tug

TODO