/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.annotations;

/**
 * Used to describe a tri-state property which is either <tt>true</tt>, <tt>false</tt> or <tt>default</tt>, which
 * will use the default value used by Elasticsearch.
 */
public enum ESOption {

    /**
     * Specifies <tt>true</tt> to elasticsearch as the value for this option
     */
    TRUE("true"),

    /**
     * Specifies <tt>false</tt> to elasticsearch as the value for this option
     */
    FALSE("false"),

    /**
     * Specifies no value to elasticsearch, hence using the default value of elasticserach
     */
    ES_DEFAULT("");

    private final String value;

    ESOption(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
