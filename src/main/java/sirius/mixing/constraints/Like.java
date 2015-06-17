/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.constraints;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Strings;
import sirius.mixing.Column;
import sirius.mixing.Constraint;
import sirius.mixing.SmartQuery;

import java.util.List;

/**
 * Created by aha on 29.04.15.
 */
public class Like extends Constraint {

    private Column field;
    private String value;
    private boolean ignoreEmpty;
    private boolean ignoreCase;

    private Like(Column field) {
        this.field = field;
    }

    public static Like on(Column field) {
        return new Like(field);
    }

    public static Constraint allWordsInAnyField(String query, Column... fields) {
        List<Constraint> wordConstraints = Lists.newArrayList();
        for (String word : query.split("\\s")) {
            List<Constraint> fieldConstraints = Lists.newArrayList();
            for (Column field : fields) {
                fieldConstraints.add(Like.on(field).contains(word).ignoreCase().ignoreEmpty());
            }
            wordConstraints.add(Or.of(fieldConstraints));
        }
        return And.of(wordConstraints);
    }

    public Like matches(String value) {
        this.value = value;
        return this;
    }

    public Like contains(String value) {
        if (Strings.isFilled(value)) {
            if (!value.startsWith("*")) {
                value = "*" + value;
            }
            if (!value.endsWith("*")) {
                value = value + "*";
            }
        }
        this.value = value;
        return this;
    }

    public Like ignoreCase() {
        this.ignoreCase = true;
        return this;
    }

    public Like ignoreEmpty() {
        this.ignoreEmpty = true;
        return this;
    }

    @Override
    public boolean addsConstraint() {
        return !ignoreEmpty || Strings.isFilled(value);
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        if (addsConstraint()) {
            String effectiveValue = value.replace('*', '%');
            if (ignoreCase) {
                compiler.getWHEREBuilder()
                        .append("LOWER(")
                        .append(compiler.translateColumnName(field))
                        .append(") LIKE ?");
                compiler.addParameter(effectiveValue.toLowerCase());
            } else {
                compiler.getWHEREBuilder().append(compiler.translateColumnName(field)).append(" LIKE ?");
                compiler.addParameter(effectiveValue);
            }
        }
    }
}
