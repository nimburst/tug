package cloud.nimburst.tug;

/**
 * The action to perform on the cluster
 */
public enum TugAction {
    /**
     * Remove resources from the cluster.
     */
    PULL,
    /**
     * Add resources to the cluster.
     */
    PUSH,
    /**
     * Remove then add resources to the cluster.
     */
    REPUSH
}
