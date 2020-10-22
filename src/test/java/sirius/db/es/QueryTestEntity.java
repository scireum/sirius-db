/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Versioned;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mongo.MangoTestEntity;
import sirius.db.mongo.types.MongoRef;

import java.time.LocalDateTime;

@Versioned
public class QueryTestEntity extends ElasticEntity {

    public static final Mapping VALUE = Mapping.named("value");
    private String value;

    public static final Mapping COUNTER = Mapping.named("counter");
    private int counter;

    public static final Mapping DATE_TIME = Mapping.named("dateTime");
    @NullAllowed
    private LocalDateTime dateTime;

    public static final Mapping MONGO_ID = Mapping.named("mongoId");
    @NullAllowed
    private final MongoRef<MangoTestEntity> mongoId = MongoRef.on(MangoTestEntity.class, BaseEntityRef.OnDelete.IGNORE);

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public MongoRef<MangoTestEntity> getMongoId() {
        return mongoId;
    }
}
