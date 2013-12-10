package mr.exceptions;

public class UnableToMoveDataException extends RuntimeException {

    public UnableToMoveDataException(String msg) {
        super(msg);
    }

    public UnableToMoveDataException(String msg, Exception e) {
        super(msg, e);
    }

    public UnableToMoveDataException(Exception ex) {
        super(ex);
    }
}
