package serverlayer.model;

public class InvalidSqlException extends Exception {
    public InvalidSqlException() {
    }

    public InvalidSqlException(String message) {
        super(message);
    }

    public InvalidSqlException(Throwable cause) {
        super(cause);
    }
}
