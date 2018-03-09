package cloud.nimburst.tug;

public interface ResourceAction {

    void makeReady() throws ResourceActionException;
    void delete() throws ResourceActionException;
}
