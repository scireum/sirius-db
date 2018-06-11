/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.GlobalContext;
import sirius.kernel.di.Initializable;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Register(classes = {Mixing.class, Initializable.class})
public class Mixing implements Initializable {

    public static final String DEFAULT_REALM = "mixing";

    @SuppressWarnings("squid:S1192")
    @Explain("Constants have different semantics.")
    public static final Log LOG = Log.get("mixing");

    private Map<Class<?>, EntityDescriptor> descriptorsByType = new HashMap<>();
    private Map<String, EntityDescriptor> descriptorsByName = new HashMap<>();

    @Part
    private GlobalContext globalContext;

    @Override
    public void initialize() throws Exception {
        descriptorsByType.clear();
        descriptorsByName.clear();
        loadEntities();
        linkSchema();
    }

    private void linkSchema() {
        descriptorsByType.values().forEach(EntityDescriptor::link);
    }

    private void loadEntities() {
        for (Class<? extends BaseEntity<?>> mappableType : EntityLoadAction.getMappableClasses()) {
            EntityDescriptor descriptor = new EntityDescriptor(mappableType);
            descriptor.initialize();
            descriptorsByType.put(mappableType, descriptor);
            String typeName = getNameForType(descriptor.getType());
            EntityDescriptor conflictingDescriptor = descriptorsByName.get(typeName);
            if (conflictingDescriptor != null) {
                Exceptions.handle()
                          .to(LOG)
                          .withSystemErrorMessage(
                                  "Cannot register mapping descriptor for '%s' as '%s' as this name is already taken by '%s'",
                                  mappableType.getName(),
                                  typeName,
                                  conflictingDescriptor.getType().getName())
                          .handle();
            } else {
                descriptorsByName.put(typeName, descriptor);
            }
        }
    }

    /**
     * Each entity type can be addressed by its class or by a unique name, which is its simple class name in upper
     * case.
     *
     * @param type the entity class to generate the type name for
     * @return the type name of the given type
     */
    @Nonnull
    public static String getNameForType(@Nonnull Class<?> type) {
        return type.getSimpleName().toUpperCase();
    }

    /**
     * Computes the unique name of an entity based on its descriptor type and id.
     *
     * @param typeName the name of the entity type
     * @param id       the id of the entity
     * @return a unique name consisting of the typeName and id
     */
    @Nonnull
    public static String getUniqueName(@Nonnull String typeName, Object id) {
        return typeName + "-" + id;
    }

    /**
     * Computes the unique name of an entity based on its type and id.
     *
     * @param type the entity class to generate the type name for
     * @param id   the id of the entity
     * @return a unique name consisting of the typeName and id
     */
    @Nonnull
    public static String getUniqueName(@Nonnull Class<?> type, Object id) {
        return Mixing.getNameForType(type) + "-" + id;
    }

    /**
     * Splits a unique name into the descriptor type and id.
     *
     * @param uniqueName the unique name of an entity.
     * @return the type and id of the entity as tuple
     * @see #getUniqueName(String, Object)
     */
    @Nonnull
    public static Tuple<String, String> splitUniqueName(@Nullable String uniqueName) {
        return Strings.split(uniqueName, "-");
    }

    /**
     * Returns the descriptor of the given entity class.
     *
     * @param aClass the entity class
     * @return the descriptor of the given entity class
     */
    public EntityDescriptor getDescriptor(Class<?> aClass) {
        EntityDescriptor ed = descriptorsByType.get(aClass);
        if (ed == null) {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("The class '%s' is not a managed entity!", aClass.getName())
                            .handle();
        }

        return ed;
    }

    /**
     * Returns the descriptor for the given entity type.
     *
     * @param aTypeName a {@link #getNameForType(Class)} of an entity
     * @return the descriptor for the given type name
     * @throws sirius.kernel.health.HandledException if no matching descriptor exists
     */
    public EntityDescriptor getDescriptor(String aTypeName) {
        Optional<EntityDescriptor> ed = findDescriptor(aTypeName);
        if (!ed.isPresent()) {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("The name '%s' is not a known entity!", aTypeName)
                            .handle();
        }

        return ed.get();
    }

    /**
     * Returns the descriptor for the given entity type.
     *
     * @param aTypeName a {@link #getNameForType(Class)} of an entity
     * @return the descriptor for the given type name as optional
     */
    public Optional<EntityDescriptor> findDescriptor(String aTypeName) {
        return Optional.ofNullable(descriptorsByName.get(aTypeName));
    }

    /**
     * Returns all known descriptors.
     *
     * @return an unmodifyable list of all known descriptors
     */
    public Collection<EntityDescriptor> getDesciptors() {
        return descriptorsByType.values();
    }
}
