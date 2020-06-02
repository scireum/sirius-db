/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

/**
 * Provides a basic tokenizer which is suitable for pre-processing content to be inserted into Elasticsearch.
 * <p>
 * Use the matching {@link BasicSearchTokenizer} to process search queries on this content.
 */
public class BasicIndexTokenizer extends Tokenizer {

    @Override
    protected ChainableTokenProcessor createProcessor() {
        return new PipelineProcessor(PatternReplaceProcessor.createRemoveControlCharacters(),
                                     new ReduceCharacterProcessor(),
                                     PatternSplitProcessor.createHardBoundarySplitter(),
                                     PatternSplitProcessor.createWhitespaceSplitter(),
                                     PatternExtractProcessor.createEmailExtractor(),
                                     PatternSplitProcessor.createSoftBoundarySplitter(),
                                     new TokenLimitProcessor(2, 80),
                                     new LowerCaseProcessor(),
                                     new DeduplicateProcessor(true));
    }
}
