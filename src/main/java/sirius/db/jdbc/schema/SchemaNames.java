/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

/**
 * Helpers for schema object names, which the database treats case-insensitively.
 */
final class SchemaNames {

    private SchemaNames() {
    }

    /**
     * Computes a hash code which is consistent with {@link String#equalsIgnoreCase(String)}.
     * <p>
     * Each code point is folded the same way {@code equalsIgnoreCase} compares it: first to upper case, then to lower
     * case. Folding the whole string via {@link String#toLowerCase(java.util.Locale)} would not suffice: e.g.
     * {@code "İ"} and {@code "i"}, or {@code "ΟΣ"} and {@code "οσ"} are equal ignoring case, but are lowercased to
     * different strings (a dotted i with a combining dot, a final sigma).
     *
     * @param name the name to hash, may be <tt>null</tt>
     * @return a hash code which is equal for all names that are equal ignoring case, 0 for <tt>null</tt>
     */
    static int hashIgnoringCase(String name) {
        if (name == null) {
            return 0;
        }
        return name.codePoints()
                   .map(codePoint -> Character.toLowerCase(Character.toUpperCase(codePoint)))
                   .reduce(0, (hash, codePoint) -> 31 * hash + codePoint);
    }
}
