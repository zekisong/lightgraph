package rocksdb.exception;

import java.io.IOException;

public class BackendException extends IOException {
    public BackendException(String msg) {
        super(msg);
    }

    public BackendException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
