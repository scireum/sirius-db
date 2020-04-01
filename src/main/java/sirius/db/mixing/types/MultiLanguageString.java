/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.kernel.nls.NLS;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Provides a language-text map as property value.
 * <p>
 * These are stored in MongoDB as an array containing sub-documents
 * containing a <tt>lang</tt> and a <tt>text</tt> property
 *
 * @see sirius.db.mixing.properties.MultiLanguageStringProperty
 */
public class MultiLanguageString extends SafeMap<String, String> {

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected String copyValue(String value) {
        return value;
    }

    /**
     * Adds a new text using the language defined by {@link NLS#getCurrentLang()}.
     *
     * @param text the text associated with the language
     * @return the object itself for fluent method calls
     * @throws sirius.kernel.health.HandledException if the current language is invalid
     */
    public MultiLanguageString addText(String text) {
        return addText(NLS.getCurrentLang(), text);
    }

    /**
     * Adds a new text for the given language.
     *
     * @param language the language code
     * @param text     the text associated with the language
     * @return the object itself for fluent method calls
     * @throws sirius.kernel.health.HandledException if the provided language code is invalid
     */
    public MultiLanguageString addText(String language, String text) {
        put(language, text);
        return this;
    }

    /**
     * Checks if a text exists for a given language.
     *
     * @param language the language code
     * @return <tt>true</tt> when a text exists, otherwise <tt>false</tt>
     */
    public boolean hasText(String language) {
        return data().containsKey(language);
    }

    /**
     * Returns an optional text associated with the current language defined by {@link NLS#getCurrentLang()}.
     *
     * @return an Optional String containing the text, otherwise an empty Optional
     */
    @Nonnull
    public Optional<String> getText() {
        return getText(NLS.getCurrentLang());
    }

    /**
     * Returns an optional text associated with a given language.
     *
     * @param language the language code
     * @return an Optional String containing the text, otherwise an empty Optional
     */
    @Nonnull
    public Optional<String> getText(String language) {
        if (!hasText(language)) {
            return Optional.empty();
        }
        return Optional.of(fetchText(language));
    }

    /**
     * Returns the text associated with the current language defined by {@link NLS#getCurrentLang()}.
     *
     * @return the text if it exists, otherwise <tt>null</tt>
     */
    @Nullable
    public String fetchText() {
        return data().get(NLS.getCurrentLang());
    }

    /**
     * Returns the text associated with a given language.
     *
     * @param language the language code
     * @return the text if it exists, otherwise <tt>null</tt>
     */
    @Nullable
    public String fetchText(String language) {
        return data().get(language);
    }

    /**
     * Returns the text associated with a given language, falling back to an alternative language when not found.
     *
     * @param language         the language code
     * @param fallbackLanguage the alternative language code
     * @return the text found under <tt>language</tt>, if none found under <tt>fallbackLanguage</tt> or <tt>null</tt> otherwise
     */
    @Nullable
    public String fetchText(String language, String fallbackLanguage) {
        return data().getOrDefault(language, fetchText(fallbackLanguage));
    }
}
