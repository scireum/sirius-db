/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

/**
 * Signals that a concurrent modification occured on an versioned entity which supports <tt>optimistic locking</tt>.
 * <p>
 * In contrast to <tt>pessimistic locking</tt>, <tt>optimistic locking</tt> does not acquire any locks or perform
 * other measures to quarantee mutual exclusion. Rather it keepts track of the entity version it last read from
 * the database and upon a modification, it expects the entity version in the database to remain the same.
 * <p>
 * Once a modification is performed, the version is then incremented. If the expected version does not match
 * the actual version, the operation is aborted and an {@link OptimisticLockException} is thrown.
 * <p>
 * This yields is a highly performant and highly scaleble system as long as concurrent modifications are rare. The
 * downsice of this approach is, that modification have to be retried once an error is detected. However most of the
 * time this overhead is quite bearable.
 */
public class OptimisticLockException extends Exception {

    private static final long serialVersionUID = -834083199170415643L;

    public OptimisticLockException() {
    }

    public OptimisticLockException(String message) {
        super(message);
    }

    public OptimisticLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public OptimisticLockException(Throwable cause) {
        super(cause);
    }
}
