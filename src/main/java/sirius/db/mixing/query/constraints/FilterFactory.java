/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.query.constraints;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Defines a factory to create common filters or constraints to be used in a {@link sirius.db.mixing.query.Query}.
 *
 * @param <C> the type of constraints being created
 */
public abstract class FilterFactory<C extends Constraint> {

    /**
     * Transforms a given value into the representation expected by the database.
     *
     * @param value the value to tranform
     * @return the transformed value
     */
    public Object transform(Object value) {
        if (value != null && value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof BaseEntity<?>) {
            return ((BaseEntity<?>) value).getId();
        }
        if (value instanceof BaseEntityRef<?, ?>) {
            return ((BaseEntityRef<?, ?>) value).getId();
        }
        if (value instanceof Value) {
            return ((Value) value).asString();
        }

        return customTransform(value);
    }

    /**
     * Permits to provive database dependent transformations.
     *
     * @param value the value to tranform
     * @return the transformed value
     */
    protected abstract Object customTransform(Object value);

    /**
     * Represents <tt>field = value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with
     * @return the generated constraint
     */
    public C eq(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return notFilled(field);
        }

        return eqValue(field, effectiveValue);
    }

    /**
     * Represents <tt>field = value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with
     * @return the generated constraint
     */

    protected abstract C eqValue(Mapping field, @Nonnull Object value);

    /**
     * Represents <tt>field = value</tt> as constraint
     * <p>
     * However, if the value is <tt>null</tt>, no constraint will be generated.
     *
     * @param field the field to filter
     * @param value the value to filter with
     * @return the generated constraint
     */
    @Nullable
    public C eqIgnoreEmpty(Mapping field, Object value) {
        Object effectiveValue = transform(value);
        if (Strings.isEmpty(effectiveValue)) {
            return null;
        }

        return eqValue(field, effectiveValue);
    }

    /**
     * Represents <tt>field = value OR empty(field)</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with
     * @return the generated constraint
     */
    public C eqOrEmpty(Mapping field, Object value) {
        return or(eq(field, value), notFilled(field));
    }

    /**
     * Represents <tt>field != value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with
     * @return the generated constraint
     */
    public C ne(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return filled(field);
        }

        return neValue(field, effectiveValue);
    }

    /**
     * Represents <tt>field != value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with
     * @return the generated constraint
     */
    protected abstract C neValue(Mapping field, @Nonnull Object value);

    /**
     * Represents <tt>field &gt; value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C gt(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return gtValue(field, effectiveValue, false);
    }

    /**
     * Represents <tt>field &gt; value</tt> as constraint
     *
     * @param field   the field to filter
     * @param value   the value to filter with
     * @param orEqual if <tt>true</tt>, <tt>field &gt;= value</tt> is used as comparator
     * @return the generated constraint
     */
    @Nonnull
    protected abstract C gtValue(Mapping field, @Nonnull Object value, boolean orEqual);

    /**
     * Represents <tt>field &gt; value OR empty(field)</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C gtOrEmpty(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return or(gtValue(field, effectiveValue, false), notFilled(field));
    }

    /**
     * Represents <tt>field &gt;= value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C gte(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return gtValue(field, effectiveValue, true);
    }

    /**
     * Represents <tt>field &gt;= value OR empty(field)</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C gteOrEmpty(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return or(gtValue(field, effectiveValue, true), notFilled(field));
    }

    /**
     * Represents <tt>field &lt; value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C lt(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return ltValue(field, effectiveValue, false);
    }

    /**
     * Represents <tt>field &lt; value</tt> as constraint
     *
     * @param field   the field to filter
     * @param value   the value to filter with
     * @param orEqual if <tt>true</tt>, <tt>field &lt;= value</tt> is used as comparator
     * @return the generated constraint
     */
    @Nonnull
    protected abstract C ltValue(Mapping field, @Nonnull Object value, boolean orEqual);

    /**
     * Represents <tt>field &lt; OR empty(field) value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C ltOrEmpty(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return or(ltValue(field, effectiveValue, false), notFilled(field));
    }

    /**
     * Represents <tt>field &lt;= value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C lte(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return ltValue(field, effectiveValue, true);
    }

    /**
     * Represents <tt>field &lt;= OR empty(field) value</tt> as constraint
     *
     * @param field the field to filter
     * @param value the value to filter with. If <tt>null</tt> is passed in, no constraint is generated
     * @return the generated constraint
     */
    @Nullable
    @SuppressWarnings("squid:S2637")
    @Explain("False positive")
    public C lteOrEmpty(Mapping field, @Nullable Object value) {
        Object effectiveValue = transform(value);
        if (effectiveValue == null) {
            return null;
        }

        return or(ltValue(field, effectiveValue, true), notFilled(field));
    }

    /**
     * Generates a constraint which ensures that the given field is filled (not null)
     *
     * @param field the field to check
     * @return the generated constraint
     */
    public abstract C filled(Mapping field);

    /**
     * Generates a constraint which ensures that the given field is not filled (null)
     *
     * @param field the field to check
     * @return the generated constraint
     */
    public abstract C notFilled(Mapping field);

    /**
     * Inverts the given constraint.
     *
     * @param constraint the constraint to invert.
     * @return the inverted constraint
     */
    @Nullable
    public C not(@Nullable C constraint) {
        if (constraint == null) {
            return null;
        }

        return invert(constraint);
    }

    /**
     * Effectively inverts the given constraint
     *
     * @param constraint the constraint to invert
     * @return the inverted constraint
     */
    @Nonnull
    protected abstract C invert(@Nonnull C constraint);

    /**
     * Combines the list of constraints using <tt>AND</tt>
     *
     * @param constraints the constraints to combine
     * @return the combined constraint
     */
    @Nullable
    public final C and(List<C> constraints) {
        List<C> effectiveConstraints = constraints.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (effectiveConstraints.isEmpty()) {
            return null;
        }

        return effectiveAnd(effectiveConstraints);
    }

    /**
     * Combines the list of constraints using <tt>AND</tt>
     *
     * @param constraints the constraints to combine
     * @return the combined constraint
     */
    @Nullable
    @SafeVarargs
    public final C and(C... constraints) {
        return and(Arrays.asList(constraints));
    }

    /**
     * Combines the list of constraints using <tt>AND</tt>
     *
     * @param effectiveConstraints the constraints to combine (where each value will be non-null)
     * @return the combined constraint
     */
    @Nonnull
    protected abstract C effectiveAnd(List<C> effectiveConstraints);

    /**
     * Combines the list of constraints using <tt>OR</tt>
     *
     * @param constraints the constraints to combine
     * @return the combined constraint
     */
    @Nullable
    public final C or(List<C> constraints) {
        List<C> effectiveConstraints = constraints.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (effectiveConstraints.isEmpty()) {
            return null;
        }

        return effectiveOr(effectiveConstraints);
    }

    /**
     * Combines the list of constraints using <tt>OR</tt>
     *
     * @param constraints the constraints to combine
     * @return the combined constraint
     */
    @Nullable
    @SafeVarargs
    public final C or(C... constraints) {
        return or(Arrays.asList(constraints));
    }

    /**
     * Combines the list of constraints using <tt>OR</tt>
     *
     * @param effectiveConstraints the constraints to combine (where each value will be non-null)
     * @return the combined constraint
     */
    @Nonnull
    protected abstract C effectiveOr(List<C> effectiveConstraints);

    /**
     * Creates a filter builder which ensures that the given field contains at least on of the given values.
     *
     * @param field  the field to filter on
     * @param values the values to check
     * @return a filter builder used to generate a constraint
     */
    public OneInField<C> containsOne(Mapping field, Object... values) {
        return new OneInField<>(this, field, Arrays.asList(values));
    }

    /**
     * Creates a filter builder which ensures that the given field contains at least on of the given values.
     *
     * @param field  the field to filter on
     * @param values the values to check
     * @return a filter builder used to generate a constraint
     */
    public OneInField<C> oneInField(Mapping field, List<?> values) {
        return new OneInField<>(this, field, values);
    }

    /**
     * Creates a constraint which ensures that the given field doesn't contain any of the given values.
     *
     * @param field  the field to filter on
     * @param values the values to check
     * @return the generated constraint
     */
    public C containsNone(Mapping field, Object... values) {
        return noneInField(field, Arrays.asList(values));
    }

    /**
     * Creates a constraint which ensures that the given field doesn't contain any of the given values.
     *
     * @param field  the field to filter on
     * @param values the values to check
     * @return the generated constraint
     */
    public C noneInField(Mapping field, List<?> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        List<C> clauses = new ArrayList<>();
        for (Object value : values) {
            clauses.add(eq(field, value));
        }

        return not(or(clauses));
    }

    /**
     * Creates a new constraint for the given field which asserts that one of the given values in the string is
     * present.
     * <p>
     * The string can have a form like A,B,C or A|B|C.
     *
     * @param field                the field to check
     * @param commaSeparatedValues the comma separated values to check for
     * @return a new constraint representing the given filter setting
     */
    public CSVFilter<C> containsAny(Mapping field, Value commaSeparatedValues) {
        return new CSVFilter<>(this, field, commaSeparatedValues.asString(), CSVFilter.Mode.CONTAINS_ANY);
    }

    /**
     * Creates a new constraint for the given field which asserts that all of the given values in the string is
     * present.
     * <p>
     * The string can have a form like A,B,C or A|B|C.
     *
     * @param field                the field to check
     * @param commaSeparatedValues the comma separated values to check for
     * @return a new constraint representing the given filter setting
     */
    public CSVFilter<C> containsAll(Mapping field, Value commaSeparatedValues) {
        return new CSVFilter<>(this, field, commaSeparatedValues.asString(), CSVFilter.Mode.CONTAINS_ALL);
    }

    /**
     * Compiles the given query for the given entity while searching in the given fields.
     *
     * @param descriptor the descriptor of the entity being searched
     * @param query      the query to compile
     * @param fields     the default fields to search in
     * @return a constraint representing the compiled query
     */
    public C queryString(EntityDescriptor descriptor, String query, QueryField... fields) {
        return queryString(descriptor, query, Arrays.asList(fields));
    }

    /**
     * Compiles the given query for the given entity while searching in the given fields.
     *
     * @param descriptor the descriptor of the entity being searched
     * @param query      the query to compile
     * @param fields     the default fields to search in
     * @return a constraint representing the compiled query
     */
    public abstract C queryString(EntityDescriptor descriptor, String query, List<QueryField> fields);
}
