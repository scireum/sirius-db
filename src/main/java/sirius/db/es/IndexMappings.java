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
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.properties.BaseMapProperty;
import sirius.db.mixing.properties.NestedListProperty;
import sirius.kernel.Sirius;
import sirius.kernel.Startable;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.settings.Extension;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Creates the mappings for all available {@link ElasticEntity Elasticsearch entities}.
 */
@Register(classes = {IndexMappings.class, Startable.class})
public class IndexMappings implements Startable {

    /**
     * Defines the dynamic mapping mode for indices, see: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html
     */
    public enum DynamicMapping {
        STRICT("strict"), FALSE("false"), TRUE("true");

        private final String mode;

        DynamicMapping(String mode) {
            this.mode = mode;
        }

        @Override
        public String toString() {
            return mode;
        }
    }

    /**
     * Mapping key used to tell ES if and how a property is stored
     */
    public static final String MAPPING_STORED = "store";

    /**
     * Mapping key used to tell ES if a property is indexed (searchable).
     */
    public static final String MAPPING_INDEX = "index";

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

    @Override
    public void started() {
        if (!elastic.isConfigured()) {
            return;
        }

        computeRoutingTable();
        checkAndUpdateIndices();
        elastic.readyFuture.success();
    }

    private void computeRoutingTable() {
        mixing.getDescriptors().stream().filter(this::isElasticEntity).forEach(this::determineRouting);
    }

    private boolean isElasticEntity(EntityDescriptor ed) {
        return ElasticEntity.class.isAssignableFrom(ed.getType());
    }

    protected void checkAndUpdateIndices() {
        if (!mixing.shouldExecuteSafeSchemaChanges()) {
            Elastic.LOG.INFO("Elastic is started without checking the database schema...");
            return;
        }

        Elastic.LOG.INFO("Elastic is starting up and checking the database schema...");

        int numSuccess = 0;
        int numFailed = 0;
        for (EntityDescriptor ed : mixing.getDescriptors()) {
            if (isElasticEntity(ed)) {
                if (setupEntity(ed)) {
                    numSuccess++;
                } else {
                    numFailed++;
                }
            }
        }

        Elastic.LOG.INFO("Setup completed: Updated %s indices / %s failures occurred", numSuccess, numFailed);
    }

    protected boolean setupEntity(EntityDescriptor ed) {
        try {
            boolean addedAlias = setupAlias(ed);

            Elastic.LOG.FINE("Updating mapping %s for %s...",
                             elastic.determineTypeName(ed),
                             ed.getType().getSimpleName());

            createMapping(ed,
                          addedAlias ? elastic.determineAlias(ed) : elastic.determineIndex(ed),
                          DynamicMapping.STRICT);
            if (!addedAlias) {
                // we couldn't setup the alias in the first place as the index didn't exist
                setupAlias(ed);
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

    private boolean setupAlias(EntityDescriptor ed) {
        if (elastic.getLowLevelClient().aliasExists(elastic.determineAlias(ed))) {
            Elastic.LOG.FINE("Alias for mapping '%s' already present.", elastic.determineTypeName(ed));
        } else {
            if (elastic.getLowLevelClient().indexExists(elastic.determineIndex(ed))) {
                createAliasForIndex(ed);
            } else {
                Elastic.LOG.FINE("Found no index to attach an alias to for mapping '%s'.",
                                 elastic.determineTypeName(ed));
                return false;
            }
        }

        return true;
    }

    private void createAliasForIndex(EntityDescriptor ed) {
        Elastic.LOG.FINE("Creating alias for index %s. ", elastic.determineIndex(ed));
        elastic.getLowLevelClient().addAlias(elastic.determineIndex(ed), elastic.determineAlias(ed));
    }

    private void determineRouting(EntityDescriptor ed) {
        ed.getProperties()
          .stream()
          .filter(p -> p.getAnnotation(RoutedBy.class).isPresent())
          .findFirst()
          .ifPresent(p -> elastic.updateRouteTable(ed, p));
    }

    /**
     * Creates the mapping for the given {@link EntityDescriptor} within the given <tt>indexName</tt>. The index is
     * created if not present.
     *
     * @param ed        the {@link EntityDescriptor} describing the mapping that should be created
     * @param indexName the name of the index in which the mapping should be created
     * @param mode      defines the setting which should be used for dynamic mappings
     */
    public void createMapping(EntityDescriptor ed, String indexName, DynamicMapping mode) {
        JSONObject mapping = new JSONObject();
        JSONObject properties = new JSONObject();
        mapping.put("dynamic", mode.toString());
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
                                  indexName,
                                  property.getName())
                          .handle();
            } else {
                JSONObject propertyInfo = new JSONObject();
                ((ESPropertyInfo) property).describeProperty(propertyInfo);
                if (property instanceof BaseMapProperty || property instanceof NestedListProperty) {
                    propertyInfo.put("dynamic", mode.toString());
                }
                properties.put(property.getPropertyName(), propertyInfo);
            }
        }

        Extension realmConfig = Sirius.getSettings().getExtension("elasticsearch.settings", ed.getRealm());
        if (!elastic.getLowLevelClient().indexExists(indexName)) {
            Elastic.LOG.FINE("Creating index %s in Elasticsearch....", indexName);
            elastic.getLowLevelClient()
                   .createIndex(indexName,
                                realmConfig.getInt("numberOfShards"),
                                realmConfig.getInt("numberOfReplicas"));
        }

        String mappingName = elastic.determineTypeName(ed);
        Elastic.LOG.FINE("Creating mapping %s for %s in index %s in Elasticsearch....",
                         mappingName,
                         ed.getType().getSimpleName(),
                         indexName);
        elastic.getLowLevelClient().putMapping(indexName, mapping);
    }

    private boolean isExcludeFromSource(Property p) {
        return p.getAnnotation(IndexMode.class).map(IndexMode::excludeFromSource).orElse(false);
    }
}
