/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.Nested;
import sirius.db.mixing.types.StringNestedMap;
import sirius.db.mongo.MongoEntity;

import java.time.LocalDateTime;

public class MongoStringNestedMapEntity extends MongoEntity {

    public static class NestedEntity extends Nested {

        private String value1;
        private LocalDateTime value2;

        public String getValue1() {
            return value1;
        }

        public LocalDateTime getValue2() {
            return value2;
        }

        public NestedEntity withValue1(String value) {
            this.value1 = value;
            return this;
        }

        public NestedEntity withValue2(LocalDateTime value) {
            this.value2 = value;
            return this;
        }
    }

    public static final Mapping MAP = Mapping.named("map");
    private final StringNestedMap<NestedEntity> map = new StringNestedMap<>(NestedEntity.class);

    public StringNestedMap<NestedEntity> getMap() {
        return map;
    }
}
