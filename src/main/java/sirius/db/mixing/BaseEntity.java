/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Maps;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Represents the base class for all entities which can be managed using {@link Mixing}.
 * <p>
 * Each field will become a property, unless it is annotated with {@link Transient}.
 * <p>
 * The framework highly encourages composition over inheritance. Therefore {@link Composite} fields will directly
 * result in the equivalent properties required to store the fields declared there. Still inheritance might be
 * useful and is fully supported for both, entities and composites.
 * <p>
 * What is not supported, is merging distinct subclasses into one table or other weired inheritance methods. Therefore
 * all superclasses should be abstract.
 * <p>
 * Additionally all <tt>Mixins</tt> {@link sirius.db.mixing.annotations.Mixin} will be used to add properties to the
 * entity. This is especially useful to extend existing entities from within customizations.
 *
 * @param <I> the type of the ID used by subclasses
 */
public abstract class BaseEntity<I> extends Mixable {

    @Part
    protected static Mixing mixing;

    @Transient
    protected Map<Property, Object> persistedData = Maps.newHashMap();

    /**
     * Contains the unique id of the entity.
     * <p>
     * Subclasses need to create a field with the name {@code id} and the type {@code <I>}.
     */
    public static final Mapping ID = Mapping.named("id");

    /**
     * Contains the constant used to mark a new (unsaved) entity.
     */
    public static final String NEW = "new";

    private static final String PARAM_FIELD = "field";

    /**
     * Returns the descriptor which maps the entity to the database table.
     *
     * @return the ddescriptor which is in charge of checking and mapping the entity to the database
     */
    public EntityDescriptor getDescriptor() {
        return mixing.getDescriptor(getClass());
    }

    /**
     * Returns the id of the entity.
     *
     * @return the id of the entity
     */
    @Nullable
    public abstract I getId();

    /**
     * Determines if the entity is new (not yet written to the database).
     * <p>
     * This wont work in {@link sirius.db.mixing.annotations.AfterSave} handlers - use {@link #wasCreated()} instead.
     *
     * @return <tt>true</tt> if the entity has not been written to the database yes, <tt>false</tt> otherwise
     */
    public boolean isNew() {
        return getId() == null;
    }

    /**
     * Each entity type can be addressed by its class or by a unique name, which is its simple class name in upper
     * case.
     *
     * @return the type name of this entity type
     * @see #getUniqueName()
     */
    public String getTypeName() {
        return Mixing.getNameForType(getClass());
    }

    /**
     * Returns an unique name of this entity.
     * <p>
     * This unique string representation of this entity is made up of its type along with its id.
     *
     * @return an unique representation of this entity or an empty string if the entity was not written to the
     * database yet
     */
    public final String getUniqueName() {
        if (isNew()) {
            return "";
        }
        return Mixing.getUniqueName(getTypeName(), getId());
    }

    /**
     * Provides the {@link BaseMapper mapper} which is used to actually manage the entity.
     *
     * @param <E> the entity type of the mapper
     * @param <Q> the query type of the mapper
     * @return the mapper which is in charge of this entity.
     */
    public abstract <E extends BaseEntity<?>, C extends Constraint, Q extends Query<Q, E, C>> BaseMapper<E, C, Q> getMapper();

    /**
     * Determines if the given value in the given field is unique within the given side constraints.
     *
     * @param field  the field to check
     * @param value  the value to be unique
     * @param within the side constraints within the value must be unique
     * @return <tt>true</tt> if the given field is unique, <tt>false</tt> otherwise
     */
    public abstract boolean isUnique(Mapping field, Object value, Mapping... within);

    /**
     * Ensures that the given value in the given field is unique within the given side constraints.
     *
     * @param field  the field to check
     * @param value  the value to be unique
     * @param within the side constraints within the value must be unique
     * @throws sirius.kernel.health.HandledException if the value isn't unique
     */
    public void assertUnique(Mapping field, Object value, Mapping... within) {
        if (!isUnique(field, value, within)) {
            throw Exceptions.createHandled()
                            .error(new InvalidFieldException(field.toString()))
                            .withNLSKey("Property.fieldNotUnique")
                            .set(PARAM_FIELD, getDescriptor().getProperty(field).getFullLabel())
                            .set("value", NLS.toUserString(value))
                            .handle();
        }
    }

    /**
     * Asserts that the given field is filled.
     * <p>
     * This can be used for conditional <tt>null</tt> checks.
     *
     * @param field the field to check
     * @see Property#isConsideredNull(Object)
     */
    public void assertNonNull(Mapping field) {
        assertNonNull(field, getDescriptor().getProperty(field).getValue(this));
    }

    /**
     * Asserts that the given field, containing the given value is filled.
     * <p>
     * This can be used for conditional <tt>null</tt> checks.
     *
     * @param field the field to check
     * @param value the value to check. Note that even a "non-null" value here, might be considered null/empty on the
     *              database layer (e.g. <tt>sirius.db.mixing.properties.AmountProperty.isConsideredNull(Object)</tt>).
     * @see Property#isConsideredNull(Object)
     */
    public void assertNonNull(Mapping field, Object value) {
        Property property = getDescriptor().getProperty(field);
        if (property.isConsideredNull(value)) {
            throwEmptyFieldError(field, property);
        }
    }

    private void throwEmptyFieldError(Mapping field, Property property) {
        throw Exceptions.createHandled()
                        .error(new InvalidFieldException(field.toString()))
                        .withNLSKey("Property.fieldNotNullable")
                        .set(PARAM_FIELD, property.getFullLabel())
                        .handle();
    }

    /**
     * Emits a validation warning if the given field is considered <tt>null</tt>.
     *
     * @param field                     the field to check
     * @param validationWarningConsumer the consumer to be supplied with validation warnings
     */
    public void validateNonNull(Mapping field, Consumer<String> validationWarningConsumer) {
        validateNonNull(field, getDescriptor().getProperty(field).getValue(this), validationWarningConsumer);
    }

    /**
     * Emits a validation warning if the given field with the given value is considered <tt>null</tt>.
     *
     * @param field                     the field to check
     * @param value                     the value to check
     * @param validationWarningConsumer the consumer to be supplied with validation warnings
     * @see #assertNonNull(Mapping)
     */
    public void validateNonNull(Mapping field, Object value, Consumer<String> validationWarningConsumer) {
        Property property = getDescriptor().getProperty(field);
        if (property.isConsideredNull(value)) {
            emitEmptyFieldWarning(validationWarningConsumer, property);
        }
    }

    private void emitEmptyFieldWarning(Consumer<String> validationWarningConsumer, Property property) {
        validationWarningConsumer.accept(NLS.fmtr("Property.fieldNotNullable")
                                            .set(PARAM_FIELD, property.getFullLabel())
                                            .format());
    }

    /**
     * Returns a string representation of the entity ID.
     * <p>
     * If the entity is new, "new" will be returned.
     *
     * @return the entity ID as string or "new" if the entity {@link #isNew()}.
     */
    public final String getIdAsString() {
        if (isNew()) {
            return NEW;
        }
        return String.valueOf(getId());
    }

    /**
     * Determines the given {@link Mapping mappings} were changed in this {@link BaseEntity} since it was last
     * fetched from the database.
     * <p>
     * If a property wears an {@link sirius.db.mixing.annotations.Trim} annotation or if "" and <tt>null</tt>
     * should be considered equal, {@link Strings#areEqual(Object, Object)}
     * or {@link Strings#areTrimmedEqual(Object, Object)} can be used as <tt>equalsFunction</tt>.
     *
     * @param mappingToCheck the columns to check whether they were changed
     * @param equalsFunction the function which compares the current and the previously persisted value and returns
     *                       <tt>true</tt> if they are equal or <tt>false</tt> otherwise
     * @return <tt>true</tt> if the requested property changed, <tt>false</tt> otherwise
     */
    public boolean isChanged(Mapping mappingToCheck, BiPredicate<? super Object, ? super Object> equalsFunction) {
        return getDescriptor().isChanged(this, getDescriptor().getProperty(mappingToCheck), equalsFunction);
    }

    /**
     * Determines if at least one of the given {@link Mapping}s were changed in this {@link BaseEntity} since it was last
     * fetched from the database.
     *
     * @param mappingsToCheck the columns to check whether they were changed
     * @return <tt>true</tt> if at least one column was changed, <tt>false</tt> otherwise
     */
    public boolean isChanged(Mapping... mappingsToCheck) {
        for (Mapping mapping : mappingsToCheck) {
            if (getDescriptor().isChanged(this, getDescriptor().getProperty(mapping))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines if the entity has just been created.
     * <p>
     * This only works if called from a {@link sirius.db.mixing.annotations.AfterSave} handler, as moments later the persisted
     * data gets updated.
     *
     * @return <tt>true</tt> if the entity has just been written to the database, <tt>false</tt> otherwise
     */
    public boolean wasCreated() {
        return isChanged(ID);
    }

    /**
     * Provides a boilerplate way of only executing a lambda if the referenced mapping has changed.
     * <p>
     * This is most probably useful for deciding if a check in a {@link sirius.db.mixing.annotations.BeforeSave}
     * handler should be executed or not.
     *
     * @param mappingToCheck the mapping to check
     * @param codeToExecute  the lambda to execute if the mapping has changed
     * @see #ifChangedAndFilled(Mapping, Runnable)
     */
    public void ifChanged(Mapping mappingToCheck, Runnable codeToExecute) {
        if (isChanged(mappingToCheck)) {
            codeToExecute.run();
        }
    }

    /**
     * Provides a boilerplate way of only executing a lambda if the referenced mapping has changed and contains a
     * non-null value.
     * <p>
     * This is most probably useful for deciding if a check in a {@link sirius.db.mixing.annotations.BeforeSave}
     * handler should be executed or not.
     *
     * @param mappingToCheck the mapping to check
     * @param codeToExecute  the lambda to execute if the mapping has changed
     * @see #ifChangedAndFilled(Mapping, Runnable)
     */
    public void ifChangedAndFilled(@Nonnull Mapping mappingToCheck, @Nonnull Runnable codeToExecute) {
        checkIfChangedOrEmpty(mappingToCheck, codeToExecute, null);
    }

    protected void checkIfChangedOrEmpty(@Nonnull Mapping mappingToCheck,
                                         @Nonnull Runnable check,
                                         @Nullable Runnable emptyHandler) {
        Property property = getDescriptor().getProperty(mappingToCheck);
        Object propertyValue = property.getValue(this);
        if (property.isConsideredNull(propertyValue)) {
            if (emptyHandler != null) {
                emptyHandler.run();
            }
            return;
        }

        if (Objects.equals(persistedData.get(property), property)) {
            return;
        }

        check.run();
    }

    /**
     * Provides a boilerplate way of executing a check if the requested field is changed and filled or otherwise emit an
     * error if the field is empty.
     *
     * @param mappingToCheck the mapping to check
     * @param codeToExecute  the lambda to execute if the mapping has changed
     * @throws sirius.kernel.health.HandledException if the field is empty
     */
    public void verifyIfChangedFailIfEmpty(@Nonnull Mapping mappingToCheck, @Nonnull Runnable codeToExecute) {
        checkIfChangedOrEmpty(mappingToCheck, codeToExecute, () -> {
            Property property = getDescriptor().getProperty(mappingToCheck);
            throwEmptyFieldError(mappingToCheck, property);
        });
    }

    /**
     * Provides a boilerplate way of executing a validation to ensure that the requested field is changed and filled
     * or otherwise emit a validation warning if the field is empty.
     *
     * @param mappingToCheck     the mapping to check
     * @param validationConsumer the validation consumer which is passed into the
     *                           {@link sirius.db.mixing.annotations.OnValidate} method.
     * @param codeToExecute      the lambda to execute if the mapping has changed
     */
    public void validateIfChangedFailIfEmpty(@Nonnull Mapping mappingToCheck,
                                             @Nonnull Consumer<String> validationConsumer,
                                             @Nonnull Consumer<Consumer<String>> codeToExecute) {
        checkIfChangedOrEmpty(mappingToCheck, () -> codeToExecute.accept(validationConsumer), () -> {
            Property property = getDescriptor().getProperty(mappingToCheck);
            emitEmptyFieldWarning(validationConsumer, property);
        });
    }

    /**
     * Outputs the persisted data for the given property.
     * <p>
     * The persisted data is the value which was/is present in the database and commonly compared to the current
     * value in the entity to perform change tracking for differential updates and journaling.
     *
     * @param property the property to lookup
     * @return the value which has been loaded from the database or <tt>null</tt> if either no value was present
     * or if the property wasn't loaded
     */
    @Nullable
    public Object getPersistedValue(Property property) {
        return persistedData.get(property);
    }

    /**
     * Checks whether any {@link Mapping} of the current {@link BaseEntity} changed.
     *
     * @return <tt>true</tt> if at least one column was changed, <tt>false</tt> otherwise.
     */
    public boolean isAnyMappingChanged() {
        return getDescriptor().getProperties().stream().anyMatch(property -> getDescriptor().isChanged(this, property));
    }

    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash tables such as those
     * provided by {@link java.util.HashMap}.
     * <p>
     * The hash code of an entity is based on its ID. If the entity is not written to the database yet, we use
     * the hash code as computed by {@link Object#hashCode()}. This matches the behaviour of {@link #equals(Object)}.
     *
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        if (isNew()) {
            // Return a hash code appropriate to the implementation of equals.
            return super.hashCode();
        }

        return getId().hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     * <p>
     * Equality of two entities is based on their type and ID. If an entity is not written to the database yet, we
     * determine equality as computed by {@link Object#equals(Object)}. This matches the behaviour of
     * {@link #hashCode()}.
     *
     * @return {@code true} if this object is the same as the obj
     * argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (this.getClass() != other.getClass()) {
            return false;
        }
        BaseEntity<?> otherEntity = (BaseEntity<?>) other;
        if (isNew()) {
            return otherEntity.isNew() && super.equals(other);
        }

        return Strings.areEqual(getId(), otherEntity.getId());
    }

    @Override
    public String toString() {
        if (isNew()) {
            return "new " + getClass().getSimpleName();
        } else {
            return getUniqueName();
        }
    }
}
