/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.annotations.IndexMode;
import sirius.db.es.annotations.RoutedBy;
import sirius.db.es.annotations.StorePerYear;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.kernel.Lifecycle;
import sirius.kernel.Sirius;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.settings.Extension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Creates the mappings for all available {@link ElasticEntity Elasticsearch entities}.
 */
@Register(classes = {IndexMappings.class, Lifecycle.class})
public class IndexMappings implements Lifecycle {

    /**
     * Mapping key used to tell ES if and how a property is stored
     */
    public static final String MAPPING_STORED = "stored";

    /**
     * Mapping key used to tell ES if and how a indexed is stored
     */
    public static final String MAPPING_INDEXED = "indexed";

    /**
     * Mapping key used to tell ES about the doc_values setting.
     */
    public static final String MAPPING_DOC_VALUES = "doc_values";

    /**
     * Mapping key used to tell ES if and how to store norms.
     */
    public static final String MAPPING_NORMS = "norms";

    /**
     * Mapping key used to tell ES the mapping type of a field.
     */
    public static final String MAPPING_TYPE = "type";

    /**
     * Mapping value used to mark a field as "keywors" meaning that it is indexed but not analyzed.
     */
    public static final String MAPPING_TYPE_KEWORD = "keyword";

    @Part
    private Mixing mixing;

    @Part
    private Elastic elastic;

    private Map<String, Boolean> checkedIndices = new ConcurrentHashMap<>();

    @Override
    public void started() {
        if (!elastic.isConfigured()) {
            return;
        }

        Elastic.LOG.INFO("Elastic is starting up and checking the database schema...");

        int numSuccess = 0;
        int numFailed = 0;
        for (EntityDescriptor ed : mixing.getDesciptors()) {
            if (ElasticEntity.class.isAssignableFrom(ed.getType())) {
                if (setupEntity(ed)) {
                    numSuccess++;
                } else {
                    numFailed++;
                }
            }
        }

        Elastic.LOG.INFO("Setup completed: Updated %s indices / %s failures occurred", numSuccess, numFailed);

        elastic.readyFuture.success();
    }

    protected boolean setupEntity(EntityDescriptor ed) {
        try {
            determineRouting(ed);
            StorePerYear storePerYear = ed.getType().getAnnotation(StorePerYear.class);
            if (storePerYear != null) {
                elastic.updateDiscriminatorTable(ed, ed.getProperty(storePerYear.value()));
            } else {
                createMapping(ed, ed.getRelationName());
            }
            return true;
        } catch (Exception e) {
            Exceptions.handle()
                      .to(Elastic.LOG)
                      .error(e)
                      .withSystemErrorMessage("Cannot create mapping for type %s - %s (%s)", ed.getType().getName())
                      .handle();
            return false;
        }
    }

    private void determineRouting(EntityDescriptor ed) {
        ed.getProperties()
          .stream()
          .filter(p -> p.getAnnotation(RoutedBy.class).isPresent())
          .findFirst()
          .ifPresent(p -> elastic.updateRouteTable(ed, p));
    }

    private void createMapping(EntityDescriptor ed, String indexName) {
        JSONObject mapping = new JSONObject();
        JSONObject properties = new JSONObject();
        mapping.put("properties", properties);

        List<String> excludes = ed.getProperties()
                                  .stream()
                                  .filter(this::isExcludeFromSource)
                                  .map(Property::getName)
                                  .collect(Collectors.toList());

        if (!excludes.isEmpty()) {
            mapping.put("_source", new JSONObject().fluentPut("excludes", excludes));
        }

        for (Property property : ed.getProperties()) {
            if (!(property instanceof ESPropertyInfo)) {
                Exceptions.handle()
                          .to(Elastic.LOG)
                          .withSystemErrorMessage(
                                  "The entity %s (%s) contains an unmappable property %s - ESPropertyInfo is not available!",
                                  ed.getType().getName(),
                                  ed.getRelationName(),
                                  property.getName())
                          .handle();
            } else {
                JSONObject propertyInfo = new JSONObject();
                ((ESPropertyInfo) property).describeProperty(propertyInfo);
                properties.put(property.getPropertyName(), propertyInfo);
            }
        }

        Extension realmConfig = Sirius.getSettings().getExtension("mixing.es.settings", ed.getRealm());
        elastic.getLowLevelClient()
               .createIndex(indexName, realmConfig.getInt("numberOfShards"), realmConfig.getInt("numberOfReplicas"));
        elastic.getLowLevelClient().putMapping(indexName, ed.getRelationName(), mapping);
    }

    private boolean isExcludeFromSource(Property p) {
        return p.getAnnotation(IndexMode.class).map(IndexMode::excludeFromSource).orElse(false);
    }

    @Override
    public void stopped() {
        // NOOP
    }

    @Override
    public void awaitTermination() {
        // NOOP
    }

    @Override
    public String getName() {
        return "index-mappings";
    }

    /**
     * Determines if the given yearly index exists.
     * <p>
     * As we might check this frequently (for queries against entities stored per year) the result is cached.
     *
     * @param name the name of the index to check.
     * @return <tt>true</tt> if an index exists, <tt>false</tt> if it hasn't been created yet
     */
    public boolean yearlyIndexExists(String name) {
        if (!checkedIndices.containsKey(name)) {
            checkedIndices.put(name, elastic.getLowLevelClient().indexExists(name));
        }

        return checkedIndices.get(name);
    }

    /**
     * Ensures that the index for the given entity type and year exists and contains the appropriate mappings.
     *
     * @param ed   the descriptor of the entity type
     * @param year the year to create the index for
     */
    public void ensureYearlyIndexExists(EntityDescriptor ed, int year) {
        String name = elastic.determineYearIndex(ed, year);
        if (yearlyIndexExists(name)) {
            return;
        }

        try {
            createMapping(ed, name);
            checkedIndices.put(name, true);
        } catch (Exception e) {
            Exceptions.handle()
                      .to(Elastic.LOG)
                      .error(e)
                      .withSystemErrorMessage("Failed to initialize dynamic index %s for %s: %s (%s)",
                                              name,
                                              ed.getType().getName())
                      .handle();
        }
    }
}
