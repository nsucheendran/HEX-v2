package mr.exceptions;

public class UnableToMoveDataException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

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
