/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Changes the index mode of the annotated field.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface IndexMode {

    /**
     * Determines whether this field should be indexed in general by elasticsearch.
     *
     * @return true if this field should be indexed or not.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-index.html">
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-index.html</a>
     */
    ESOption indexed() default ESOption.ES_DEFAULT;

    /**
     * Determines whether this field should be stored separately from the _source field by elasticsearch.
     *
     * @return true if this field should be stored separately from the _source field by elasticsearch.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-store.html">
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-store.html</a>
     */
    ESOption stored() default ESOption.ES_DEFAULT;

    /**
     * Only for {@link String} and {@link java.util.List}&lt;{@link String}&gt; fields.
     * Permits to specify if norms are enabled for this field or not.
     *
     * @return whether norms are enabled or not.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html">
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html</a>
     */
    ESOption normsEnabled() default ESOption.ES_DEFAULT;

    /**
     * Permits to specify if the contents of this field are stored on disk in a column-stride fashion.
     *
     * @return <tt>"true"</tt> if the contents of this field should be stored on disk in a column-stride fashion,
     * <tt>"false"</tt> otherwise.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/doc-values.html">
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/doc-values.html</a>
     */
    ESOption docValues() default ESOption.ES_DEFAULT;

    /**
     * Permits to exclude the contents of this field from the _source field. This should be used with care!
     * <p>
     * See the elasticsearch docs for a detailed description of the behaviour.
     *
     * @return <tt>true</tt> if the value is excluded from source, <tt>false</tt> otherwise
     */
    boolean excludeFromSource() default false;
}
