package org.pih.petl;

public class PetlException extends RuntimeException {

    public PetlException() {
        super();
    }

    public PetlException(String message) {
        super(message);
    }

    public PetlException(String message, Throwable cause) {
        super(message, cause);
    }

    public PetlException(Throwable cause) {
        super(cause);
    }
}
