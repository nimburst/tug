package cloud.nimburst.tug;

/**
 * An exception indicating a resource could not be created or destroyed.
 */
public class ResourceActionException extends Exception {

    private static final long serialVersionUID = 1578271824175625278L;

    /**
     * Instantiates a new Resource action exception.
     *
     * @param message the message
     */
    public ResourceActionException(String message) {

        super(message);
    }

    /**
     * Instantiates a new Resource action exception.
     *
     * @param message the message
     * @param cause   the cause
     */
    public ResourceActionException(String message, Throwable cause) {

        super(message, cause);
    }
}
