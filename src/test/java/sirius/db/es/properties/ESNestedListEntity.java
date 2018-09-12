/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import sirius.db.es.ElasticEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Nested;
import sirius.db.mixing.types.NestedList;

public class ESNestedListEntity extends ElasticEntity {

    public static class NestedEntity extends Nested {

        private static final Mapping VALUE1 = Mapping.named("value1");
        private String value1;
        private static final Mapping VALUE2 = Mapping.named("value2");
        private String value2;

        public String getValue1() {
            return value1;
        }

        public String getValue2() {
            return value2;
        }

        public NestedEntity withValue1(String value) {
            this.value1 = value;
            return this;
        }

        public NestedEntity withValue2(String value) {
            this.value2 = value;
            return this;
        }
    }

    public static final Mapping LIST = Mapping.named("list");
    private final NestedList<NestedEntity> list = new NestedList<>(NestedEntity.class);

    public NestedList<NestedEntity> getList() {
        return list;
    }
}
