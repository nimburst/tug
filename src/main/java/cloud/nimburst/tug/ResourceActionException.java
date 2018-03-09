package cloud.nimburst.tug;

public class ResourceActionException extends Exception {

    private static final long serialVersionUID = 1578271824175625278L;

    public ResourceActionException(String message) {

        super(message);
    }

    public ResourceActionException(String message, Throwable cause) {

        super(message, cause);
    }
}
