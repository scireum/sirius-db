/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.kernel.Sirius;
import sirius.kernel.health.Exceptions;

import java.util.List;
import java.util.Optional;

/**
 * Provides a language-text map as property value.
 * <p>
 * These are stored in MongoDB as an array containing sub-documents
 * containing a <tt>lang</tt> and <tt>text</tt> properties
 *
 * @see sirius.db.mixing.properties.MultiLanguageStringProperty
 */
public class MultiLanguageString extends SafeMap<String, String> {

    private static final List<String> supportedLanguages =
            Sirius.getSettings().getConfig("mongo").getStringList("supportedLanguages");

    private String defaultLanguage;

    /**
     * Creates a new multi-language string property without default language.
     */
    public MultiLanguageString() {
        this(null);
    }

    /**
     * Creates a new multi-language string property with default language.
     *
     * @param defaultLanguage the default language code
     */
    public MultiLanguageString(String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
    }

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected String copyValue(String value) {
        return value;
    }

    /**
     * Adds a new text for the given language.
     *
     * @param language the language code
     * @param text     the text associated with the language
     * @return the object itself for fluent method calls
     */
    public MultiLanguageString addText(String language, String text) {
        assertLanguage(language);
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
     * Returns the text associated with the default language.
     *
     * @return the text
     */
    public String getRequiredText() {
        return getRequiredText(defaultLanguage);
    }

    /**
     * Returns the text associated with a given language.
     *
     * @param language the language code
     * @return the text or an {@link sirius.kernel.health.HandledException} if not existent
     */
    public String getRequiredText(String language) {
        if (!hasText(language)) {
            throw Exceptions.createHandled()
                            .withNLSKey("MultiLanguageString.textDoesNotExist")
                            .set("language", language)
                            .handle();
        }
        return fetchText(language);
    }

    /**
     * Returns an optional text associated with the default language.
     *
     * @return an Optional String containing the text, otherwise an empty Optional
     */
    public Optional<String> getText() {
        return getText(defaultLanguage);
    }

    /**
     * Returns an optional text associated with a given language.
     *
     * @param language the language code
     * @return an Optional String containing the text, otherwise an empty Optional
     */
    public Optional<String> getText(String language) {
        if (!hasText(language)) {
            return Optional.empty();
        }
        return Optional.of(fetchText(language));
    }

    /**
     * Returns the text associated with the default language.
     *
     * @return the text if it exists, otherwise <tt>null</tt>
     */
    public String fetchText() {
        return data().get(defaultLanguage);
    }

    /**
     * Returns the text associated with a given language.
     *
     * @param language the language code
     * @return the text if it exists, otherwise <tt>null</tt>
     */
    public String fetchText(String language) {
        return data().get(language);
    }

    /**
     * Returns the text associated with a given language, falling back to an alternative language when not found.
     *
     * @param language         the language code
     * @param fallbackLanguage the alternative language code
     * @return the text found under <tt>language</tt>, if none found under <tt>fallbackLanguage</tt>, <tt>null</tt> otherwise
     */
    public String fetchText(String language, String fallbackLanguage) {
        return data().getOrDefault(language, data().getOrDefault(fallbackLanguage, fetchText(defaultLanguage)));
    }

    private void assertLanguage(String language) {
        if (!supportedLanguages.contains(language)) {
            throw Exceptions.createHandled()
                            .withNLSKey("MultiLanguageString.invalidLanguage")
                            .set("language", language)
                            .handle();
        }
    }

    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    public void setDefaultLanguage(String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
    }
}
