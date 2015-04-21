/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.mixing.Composite;
import sirius.mixing.annotations.Length;

public class TestCompositeWithComposite extends Composite {

   private final TestComposite composite = new TestComposite();

    public TestComposite getComposite() {
        return composite;
    }
}
