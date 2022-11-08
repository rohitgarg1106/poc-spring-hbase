package poc.experimentation.springhbase.pocspringhbase.exception;

public class HBaseTableExistsException extends Exception{
    public HBaseTableExistsException(String message) {
        super(message);
    }
}
