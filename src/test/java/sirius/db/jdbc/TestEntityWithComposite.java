/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

public class TestEntityWithComposite extends SQLEntity {

    private final TestComposite composite = new TestComposite();

    private final TestCompositeWithComposite compositeWithComposite = new TestCompositeWithComposite();

    public TestComposite getComposite() {
        return composite;
    }

    public TestCompositeWithComposite getCompositeWithComposite() {
        return compositeWithComposite;
    }
}
