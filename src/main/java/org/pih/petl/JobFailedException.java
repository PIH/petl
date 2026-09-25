package org.pih.petl;

/**
 * Indicates that a job failed, where the failure has already been logged with its stack trace by the job in which
 * it originated.  Parent jobs and callers that receive this exception log a single line rather than the stack trace.
 */
public class JobFailedException extends PetlException {

    public JobFailedException(String message) {
        super(message);
    }

    public JobFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
