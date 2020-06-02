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
 *
 * @see sirius.db.mixing.properties.MultiLanguageStringProperty
 */
public class MultiLanguageString extends SafeMap<String, String> {

    public static final String FALLBACK_KEY = "fallback";

    private boolean withFallback;

    /**
     * Creates a new object to hold a language-text map with no place for a fallback string.
     */
    public MultiLanguageString() {
        this.withFallback = false;
    }

    /**
     * Creates a new object to hold a language-text map.
     *
     * @param withFallback if a fallback should also be stored in the map
     */
    public MultiLanguageString(boolean withFallback) {
        this.withFallback = withFallback;
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
     * Adds the given text as a fallback to the map.
     *
     * @param text the text to be used as fallback
     * @return the object itself for fluent method calls
     * @throws IllegalStateException if this field does not support fallbacks
     */
    public MultiLanguageString addFallback(String text) {
        if (!withFallback) {
            throw new IllegalStateException(
                    "Can not call addFallback on a MultiLanguageString without fallback enabled.");
        }
        return addText(FALLBACK_KEY, text);
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
     * <p>
     * If no text for the language exists and a fallback is defined, the fallback is returned.
     *
     * @return an Optional String containing the text, otherwise an empty Optional
     */
    @Nonnull
    public Optional<String> getText() {
        return getText(NLS.getCurrentLang());
    }

    /**
     * Returns an optional text associated with a given language.
     * <p>
     * If no text for the language exists and a fallback is defined, the fallback is returned.
     *
     * @param language the language code
     * @return an Optional String containing the text, otherwise an empty Optional
     */
    @Nonnull
    public Optional<String> getText(String language) {
        if (!hasText(language)) {
            if (hasFallback()) {
                return Optional.of(data.get(FALLBACK_KEY));
            }
            return Optional.empty();
        }
        return Optional.of(fetchText(language));
    }

    /**
     * Returns the text associated with the current language defined by {@link NLS#getCurrentLang()}.
     * <p>
     * Please note that the defined fallback is never used, use {@link #fetchTextOrFallback()} instead.
     *
     * @return the text if it exists, otherwise <tt>null</tt>
     */
    @Nullable
    public String fetchText() {
        return data().get(NLS.getCurrentLang());
    }

    /**
     * Returns the text associated with a given language.
     * <p>
     * Please note that the defined fallback is never used, use {@link #fetchTextOrFallback(String)} instead.
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

    /**
     * Returns the text associated with with the current language defined by {@link NLS#getCurrentLang()}, falling back to the saved fallback.
     *
     * @return the text found under <tt>language</tt>, if none found the one from {@link #FALLBACK_KEY} is returned
     */
    @Nullable
    public String fetchTextOrFallback() {
        return fetchTextOrFallback(NLS.getCurrentLang());
    }

    /**
     * Returns the text associated with a given language, falling back to the saved fallback.
     *
     * @param language the language code
     * @return the text found under <tt>language</tt>, if none found the one from {@link #FALLBACK_KEY} is returned
     */
    @Nullable
    public String fetchTextOrFallback(String language) {
        if (!withFallback) {
            throw new IllegalStateException(
                    "Can not call fetchTextOrFallback on a MultiLanguageString without fallback enabled.");
        }
        return data().getOrDefault(language, data.get(FALLBACK_KEY));
    }

    private boolean hasFallback() {
        return withFallback && containsKey(FALLBACK_KEY);
    }
}
