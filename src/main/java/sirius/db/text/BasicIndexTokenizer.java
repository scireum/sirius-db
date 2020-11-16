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
        // ATTENTION: The pipeline used to contain a "new ReduceCharacterProcessor()" stage. This lead to the search
        // index containing normalised strings, unlike earlier verions of sirius-db. Consequently, MEMOIO was no longer
        // able to find search terms containing umlauts. Weighing the two possible options — keeping the normalised
        // index and normalising the search term, or reverting the index – the decision was made to remove search index
        // normalisation.
        // --- Stage removed by JVO on 2020/11/16 after discussion with AHA

        return new PipelineProcessor(PatternReplaceProcessor.createRemoveControlCharacters(),
                                     PatternSplitProcessor.createHardBoundarySplitter(),
                                     PatternSplitProcessor.createWhitespaceSplitter(),
                                     PatternExtractProcessor.createEmailExtractor(),
                                     PatternSplitProcessor.createSoftBoundarySplitter(),
                                     new TokenLimitProcessor(2, 80),
                                     new ToLowercaseProcessor(),
                                     new DeduplicateProcessor(true));
    }
}
