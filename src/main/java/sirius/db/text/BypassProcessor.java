/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Permits to by-pass one or more token processors to also keep the original token.
 * <p>
 * This also manages purges, so that internal purges are only forwarded if an external purge happens. This permits to
 * i.e. properly enforce word boundaries when working with processors which emit purges internally.
 */
public class BypassProcessor extends ChainableTokenProcessor {

    private final ChainableTokenProcessor tokenProcessor;
    private final AtomicBoolean innerPurge = new AtomicBoolean();
    private final AtomicBoolean permitPurge = new AtomicBoolean();

    /**
     * Creates a new instance for the given processor.
     * <p>
     * If more than one processor is to be by-passed, use a {@link PipelineProcessor} as parameter
     *
     * @param tokenProcessor the processor to by-pass (each token will be directly emitted and the results of the
     *                       processor will be emitted as well).
     */
    public BypassProcessor(ChainableTokenProcessor tokenProcessor) {
        super();
        this.tokenProcessor = tokenProcessor;
        tokenProcessor.chain(new TokenProcessor() {

            @Override
            public void accept(String token) {
                emit(token);
            }

            @Override
            public void purge() {
                if (permitPurge.get()) {
                    downstream.purge();
                }
                innerPurge.set(true);
            }
        });
    }

    @Override
    public void accept(String token) {
        innerPurge.set(false);
        tokenProcessor.accept(token);
        emit(token);
        if (innerPurge.get()) {
            downstream.purge();
            permitPurge.set(false);
        } else {
            permitPurge.set(true);
        }
    }

    @Override
    public void purge() {
        permitPurge.set(true);
        tokenProcessor.purge();
        permitPurge.set(false);
    }
}
