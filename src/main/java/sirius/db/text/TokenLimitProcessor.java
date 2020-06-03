/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

/**
 * Rejects tokens which are either too long or to too short to be processed.
 */
public class TokenLimitProcessor extends ChainableTokenProcessor {

    private final int tokenMinLength;
    private final int tokenMaxLength;

    /**
     * Creates a new processor with the given lengths.
     *
     * @param tokenMinLength the minimal length for a token to be accepted
     * @param tokenMaxLength the maximal length for a token to be accepted
     */
    public TokenLimitProcessor(int tokenMinLength, int tokenMaxLength) {
        this.tokenMinLength = tokenMinLength;
        this.tokenMaxLength = tokenMaxLength;
    }

    @Override
    public void accept(String value) {
        if (tokenMinLength > 0 && value.length() < tokenMinLength) {
            return;
        }

        if (tokenMaxLength > 0 && value.length() > tokenMaxLength) {
            return;
        }

        emit(value);
    }
}
