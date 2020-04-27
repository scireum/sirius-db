/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.Mapping;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class DateEntity extends SQLEntity {
    public static final Mapping LOCAL_DATE_TIME = Mapping.named("localDateTime");
    private LocalDateTime localDateTime;
    public static final Mapping LOCAL_TIME = Mapping.named("localTime");
    private LocalTime localTime;
    public static final Mapping LOCAL_DATE = Mapping.named("localDate");
    private LocalDate localDate;

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public LocalTime getLocalTime() {
        return localTime;
    }

    public LocalDate getLocalDate() {
        return localDate;
    }
}
