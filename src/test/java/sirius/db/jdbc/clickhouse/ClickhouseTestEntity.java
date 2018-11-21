/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.clickhouse;

import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Realm;

import java.time.Instant;
import java.time.LocalDate;

@Realm("clickhouse")
public class ClickhouseTestEntity extends SQLEntity {

    public static final Mapping DATE_TIME = Mapping.named("dateTime");
    private Instant dateTime;

    public static final Mapping DATE = Mapping.named("date");
    private LocalDate date;

    public static final Mapping INT8 = Mapping.named("int8");
    @Length(1)
    private int int8;

    public static final Mapping INT16 = Mapping.named("int16");
    @Length(2)
    private int int16;

    public static final Mapping INT32 = Mapping.named("int32");
    @Length(4)
    private int int32;

    public static final Mapping INT64 = Mapping.named("int64");
    private long int64;

    public static final Mapping STRING = Mapping.named("string");
    private String string;

    public static final Mapping FIXED_STRING = Mapping.named("fixedString");
    @Length(1)
    private String fixedString;

    public static final Mapping INT8_WITH_DEFAULT = Mapping.named("int8");
    @Length(1)
    @NullAllowed
    @DefaultValue("42")
    private Integer int8WithDefault;

    public static final Mapping A_BOOLEAN_SET_TO_TRUE = Mapping.named("aBooleanSetToTrue");
    private boolean aBooleanSetToTrue;

    public static final Mapping A_BOOLEAN_SET_TO_FALSE = Mapping.named("aBooleanSetToFalse");
    private boolean aBooleanSetToFalse;

    public Instant getDateTime() {
        return dateTime;
    }

    public void setDateTime(Instant dateTime) {
        this.dateTime = dateTime;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public int getInt8() {
        return int8;
    }

    public void setInt8(int int8) {
        this.int8 = int8;
    }

    public int getInt16() {
        return int16;
    }

    public void setInt16(int int16) {
        this.int16 = int16;
    }

    public int getInt32() {
        return int32;
    }

    public void setInt32(int int32) {
        this.int32 = int32;
    }

    public long getInt64() {
        return int64;
    }

    public void setInt64(long int64) {
        this.int64 = int64;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public String getFixedString() {
        return fixedString;
    }

    public void setFixedString(String fixedString) {
        this.fixedString = fixedString;
    }

    public int getInt8WithDefault() {
        return int8WithDefault;
    }

    public void setInt8WithDefault(int int8WithDefault) {
        this.int8WithDefault = int8WithDefault;
    }

    public boolean isaBooleanSetToTrue() {
        return aBooleanSetToTrue;
    }

    public void setaBooleanSetToTrue(boolean aBooleanSetToTrue) {
        this.aBooleanSetToTrue = aBooleanSetToTrue;
    }

    public boolean isaBooleanSetToFalse() {
        return aBooleanSetToFalse;
    }

    public void setaBooleanSetToFalse(boolean aBooleanSetToFalse) {
        this.aBooleanSetToFalse = aBooleanSetToFalse;
    }
}
