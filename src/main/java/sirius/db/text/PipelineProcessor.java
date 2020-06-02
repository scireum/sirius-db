/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Permits to combine several chainable token processors into a single one.
 */
public class PipelineProcessor extends ChainableTokenProcessor {

    private List<ChainableTokenProcessor> processors;

    /**
     * Creates a new procssor.
     *
     * @param processors the internal processors the chain together to form the desired pipeline
     */
    public PipelineProcessor(List<ChainableTokenProcessor> processors) {
        this.processors = Collections.synchronizedList(processors);
        for (int i = 0; i < processors.size() - 1; i++) {
            processors.get(i).chain(processors.get(i + 1));
        }
    }

    /**
     * Creates a new procssor.
     *
     * @param processors the internal processors the chain together to form the desired pipeline
     */
    public PipelineProcessor(ChainableTokenProcessor... processors) {
        this(Arrays.asList(processors));
    }

    @Override
    public void accept(String token) {
        processors.get(0).accept(token);
    }

    @Override
    public void purge() {
        processors.get(0).purge();
    }

    @Override
    public void chain(TokenProcessor downstream) {
        processors.get(processors.size() - 1).chain(downstream);
    }

    @Override
    public void chainConsumer(Consumer<String> downstream) {
        processors.get(processors.size() - 1).chainConsumer(downstream);
    }
}
