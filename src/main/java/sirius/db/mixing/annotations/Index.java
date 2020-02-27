/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.schema.Schema;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mongo.Mango;
import sirius.db.mongo.MongoEntity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines an additional index on an {@link SQLEntity} or {@link MongoEntity}.
 * <p>
 * Note that indices of parent entities will also be picked up and applied, unless an index for the same name
 * has already been defined. If you want to suppress an index from a parent entity, simply define an index with
 * the same name and supply an empty columns list.
 * <p>
 * The index will be picked up and created by {@link Schema#computeRequiredSchemaChanges()} or
 * {@link Mango#createIndices(EntityDescriptor)}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Indices.class)
public @interface Index {

    /**
     * Contains the name of the index.
     *
     * @return the name of the index
     */
    String name();

    /**
     * Contains the columns being indexed.
     *
     * @return the columns which make up the index
     */
    String[] columns();

    /**
     * Contains a list of column settings to be used for <tt>Mongo DB</tt>.
     * <p>
     * Use {@link Mango#INDEX_ASCENDING} or {@link Mango#INDEX_DESCENDING} for
     * common use cases. Use appropriate strings determined by the Mongo DB documentation for special
     * indices (e.g. geospatial ones).
     *
     * @return an array of strings which determines the index setting per column in the {@link #columns()} array
     */
    String[] columnSettings() default {};

    /**
     * Determines if the index implies an unique constraint on the combination of the given columns.
     *
     * @return <tt>true</tt> if an unique constraint is implied, <tt>false</tt> otherwise
     */
    boolean unique() default false;
}
