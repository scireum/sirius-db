/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.es.types.ElasticRef;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.properties.BaseEntityRefProperty;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents a reference to another {@link ElasticEntity} field within a {@link Mixable}.
 */
public class ElasticRefProperty extends BaseEntityRefProperty<String, ElasticEntity, ElasticRef<ElasticEntity>>
        implements SQLPropertyInfo, ESPropertyInfo {

    @Part
    private static Elastic elastic;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return ElasticRef.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            if (!Modifier.isFinal(field.getModifiers())) {
                Mixing.LOG.WARN("Field %s in %s is not final! This will probably result in errors.",
                                field.getName(),
                                field.getDeclaringClass().getName());
            }

            propertyConsumer.accept(new ElasticRefProperty(descriptor, accessPath, field));
        }
    }

    @SuppressWarnings("unchecked")
    protected ElasticRefProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public void contributeToTable(Table table) {
        TableColumn tableColumn = new TableColumn(this, Types.CHAR);
        tableColumn.setLength(getLength() == 0 ? 40 : getLength());
        table.getColumns().add(tableColumn);
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "keyword");
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_INDEXED, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES, IndexMode::indexed, ESOption.ES_DEFAULT, description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Optional<ElasticEntity> find(Class<ElasticEntity> type, Value value) {
        return elastic.find(type, value.get());
    }
}
