package cloud.nimburst.tug;

/**
 * A ResourceAction executes resource lifecycle actions in the Kubernetes cluster.
 */
public interface ResourceAction {

    /**
     * Creates and waits for a resource to be in a ready state.
     *
     * @throws ResourceActionException if an error occurs when deploying the resource
     */
    void makeReady() throws ResourceActionException;

    /**
     * Deletes the resource.
     *
     * @throws ResourceActionException if an error occurs when deleting the resource
     */
    void delete() throws ResourceActionException;
}
