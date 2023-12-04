/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Permits to combine several chainable token processors into a single one.
 */
public class PipelineProcessor extends ChainableTokenProcessor {

    private List<ChainableTokenProcessor> processors;

    /**
     * Creates a new processor.
     *
     * @param processors the internal processors to chain together to form the desired pipeline
     */
    public PipelineProcessor(List<ChainableTokenProcessor> processors) {
        this(processors.stream());
    }

    /**
     * Creates a new processor.
     *
     * @param processorsStream the internal processors to chain together to form the desired pipeline
     */
    public PipelineProcessor(Stream<ChainableTokenProcessor> processorsStream) {
        this.processors = processorsStream.filter(Objects::nonNull).toList();
        for (int i = 0; i < processors.size() - 1; i++) {
            processors.get(i).chain(processors.get(i + 1));
        }
    }

    /**
     * Creates a new processor.
     *
     * @param processors the internal processors to chain together to form the desired pipeline
     */
    public PipelineProcessor(ChainableTokenProcessor... processors) {
        this(Stream.of(processors));
    }

    @Override
    public void accept(String token) {
        processors.getFirst().accept(token);
    }

    @Override
    public void purge() {
        processors.getFirst().purge();
    }

    @Override
    public void chain(TokenProcessor downstream) {
        processors.getLast().chain(downstream);
    }

    @Override
    public void chainConsumer(Consumer<String> downstream) {
        processors.getLast().chainConsumer(downstream);
    }
}
