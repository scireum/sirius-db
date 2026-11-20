/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;

/**
 * Entity with a payload which is neither indexed nor stored as doc value, so that huge documents can be written
 * in order to test the request size limit of {@link BulkContext}.
 */
public class LargeBatchTestEntity extends ElasticEntity {

    public static final Mapping PAYLOAD = Mapping.named("payload");
    @NullAllowed
    @Length(Integer.MAX_VALUE)
    @IndexMode(indexed = ESOption.FALSE, docValues = ESOption.FALSE)
    private String payload;

    public String getPayload() {
        return payload;
    }

    public LargeBatchTestEntity withPayload(String payload) {
        this.payload = payload;
        return this;
    }
}
