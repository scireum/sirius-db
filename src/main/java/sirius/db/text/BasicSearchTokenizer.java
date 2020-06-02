/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

/**
 * Provides a tokenizer used to parse search input for content which has been pre-processed by the
 * {@link BasicIndexTokenizer}.
 */
public class BasicSearchTokenizer extends Tokenizer {

    @Override
    protected ChainableTokenProcessor createProcessor() {
        return new PipelineProcessor(new ReduceCharacterProcessor(),
                                     PatternSplitProcessor.createHardBoundarySplitter(),
                                     PatternSplitProcessor.createWhitespaceSplitter(),
                                     new TokenLimitProcessor(2, 80),
                                     new LowerCaseProcessor(),
                                     new DeduplicateProcessor(true));
    }
}
