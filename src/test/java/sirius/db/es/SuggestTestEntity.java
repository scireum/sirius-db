/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.es.annotations.Analyzed;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;

public class SuggestTestEntity extends ElasticEntity {

    public static final Mapping CONTENT = Mapping.named("content");
    @Analyzed(analyzer = Analyzed.ANALYZER_WHITESPACE, indexOptions = Analyzed.IndexOption.POSITIONS)
    private String content;

    public static final Mapping SHOP = Mapping.named("shop");
    @NullAllowed
    private Long shop;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Long getShop() {
        return shop;
    }

    public void setShop(long shop) {
        this.shop = shop;
    }
}
