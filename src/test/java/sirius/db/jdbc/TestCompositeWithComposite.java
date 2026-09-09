/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Composite;

public class TestCompositeWithComposite extends Composite {

    private final TestComposite composite = new TestComposite();

    public TestComposite getComposite() {
        return composite;
    }
}
