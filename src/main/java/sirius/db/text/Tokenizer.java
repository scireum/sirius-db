/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Provides a base class for all tokenizers.
 * <p>
 * A tokenizer uses one or more {@link TokenProcessor} to transform strings (tokens) into a list of sub tokens
 * which are then suitable to be indexed or searched in a fulltext search engine.
 */
public abstract class Tokenizer {

    private ChainableTokenProcessor processor;
    private Consumer<List<String>> currentSink;

    protected Tokenizer() {
        this.processor = createProcessor();
        this.processor.chain(new TokenProcessor() {

            private List<String> buffer;

            @Override
            public void accept(String s) {
                if (buffer == null) {
                    buffer = new ArrayList<>();
                }
                buffer.add(s);
            }

            @Override
            public void purge() {
                if (buffer != null) {
                    currentSink.accept(buffer);
                    buffer = null;
                }
            }
        });
    }

    /**
     * Creates the internal token processor.
     * <p>
     * Most probably this will be a {@link PipelineProcessor} which wraps a sequence of other processors.
     *
     * @return the token processor which make up this tokenizer
     */
    protected abstract ChainableTokenProcessor createProcessor();

    /**
     * Processes the given collection of input tokens into 0..N lists of output tokens.
     *
     * @param input  the tokens to process
     * @param output a consumer which is supplied with lists of tokens. Each list represents one input token.
     */
    public void accept(Iterable<String> input, Consumer<List<String>> output) {
        this.currentSink = output;
        input.forEach(processor);
        processor.purge();
    }
}
