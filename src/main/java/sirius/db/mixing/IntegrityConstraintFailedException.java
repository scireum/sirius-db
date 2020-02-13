/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

/**
 * Wraps all database specific exceptions to signal that an integrity constraint was violated.
 * <p>
 * Sometimes we use a constraint like "UNIQUE" to detect certain race conditions or other parallel behaviour
 * without the need for locking. The occurrence of such an error then needs to be handled properly
 * (i.e. by retrying an operation). Therefore some methods (e.g. {@link BaseMapper#tryUpdate(BaseEntity)}
 * throw this dedicated exception to permit the application code to handle this case properly.
 */
public class IntegrityConstraintFailedException extends Exception {

    private static final long serialVersionUID = -3178562817868475776L;

    /**
     * Creates a new instance without any reference to another error.
     */
    public IntegrityConstraintFailedException() {
    }

    /**
     * Creates a new instance with a reference to the given cause.
     *
     * @param cause   the cuase of this error
     */
    public IntegrityConstraintFailedException(Throwable cause) {
        super(cause);
    }
}
