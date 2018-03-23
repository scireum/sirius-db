/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

/**
 * For testing the saving of complex values in RedisCache.
 */
public class RedisCacheTestValue {

    private String field1;

    private Long field2;

    private boolean field3;

    public RedisCacheTestValue() {

    }

    public RedisCacheTestValue(String field1, Long field2, boolean field3) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
    }

    public String getField1() {
        return field1;
    }

    public Long getField2() {
        return field2;
    }

    public boolean isField3() {
        return field3;
    }

    public void setField1(String field1) {
        this.field1 = field1;
    }

    public void setField2(Long field2) {
        this.field2 = field2;
    }

    public void setField3(boolean field3) {
        this.field3 = field3;
    }
}
