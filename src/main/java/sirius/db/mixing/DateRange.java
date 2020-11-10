/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.nls.NLS;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Represents a date range which will collect and count all matching entities in a query to provide
 * an appropriate facet filter.
 */
@Immutable
public class DateRange {

    private final String key;
    private final String name;
    private final Supplier<LocalDateTime> fromSupplier;
    private final Supplier<LocalDateTime> untilSupplier;

    /**
     * Creates a new DateRange with the given unique key, translated (shown) name and two date suppliers
     * specifying the range
     *
     * @param key           the unique name of the ranged used as filter value
     * @param name          the translated name shown to the user
     * @param fromSupplier  the lower limit (including) of the range
     * @param untilSupplier the upper limit (including) of the range
     */
    public DateRange(String key,
                     String name,
                     @Nullable Supplier<LocalDateTime> fromSupplier,
                     @Nullable Supplier<LocalDateTime> untilSupplier) {
        this.key = key;
        this.name = name;
        this.fromSupplier = fromSupplier;
        this.untilSupplier = untilSupplier;
    }

    /**
     * Creates a date range filtering on the last five minutes.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastFiveMinutes() {
        return new DateRange("5m",
                             NLS.get("DateRange.5m"),
                             () -> LocalDateTime.now().minusMinutes(5),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on the last 15 minutes.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastFiveteenMinutes() {
        return new DateRange("15m",
                             NLS.get("DateRange.15m"),
                             () -> LocalDateTime.now().minusMinutes(15),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on the last hour.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastHour() {
        return new DateRange("1h",
                             NLS.get("DateRange.1h"),
                             () -> LocalDateTime.now().minusHours(1),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on the last two hours.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastTwoHours() {
        return new DateRange("2h",
                             NLS.get("DateRange.2h"),
                             () -> LocalDateTime.now().minusHours(2),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on "today".
     *
     * @return a date range for the given interval
     */
    public static DateRange today() {
        return new DateRange("today",
                             NLS.get("DateRange.today"),
                             () -> LocalDate.now().atStartOfDay(),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on "yesterday".
     *
     * @return a date range for the given interval
     */
    public static DateRange yesterday() {
        return new DateRange("yesterday",
                             NLS.get("DateRange.yesterday"),
                             () -> LocalDate.now().minusDays(1).atStartOfDay(),
                             () -> LocalDate.now().minusDays(1).atTime(23, 59));
    }

    /**
     * Creates a date range filtering on this week.
     *
     * @return a date range for the given interval
     */
    public static DateRange thisWeek() {
        return new DateRange("thisWeek",
                             NLS.get("DateRange.thisWeek"),
                             () -> LocalDate.now()
                                            .with(WeekFields.of(Locale.getDefault()).dayOfWeek(), 1)
                                            .atStartOfDay(),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on the last week.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastWeek() {
        return new DateRange("lastWeek",
                             NLS.get("DateRange.lastWeek"),
                             () -> LocalDate.now()
                                            .minusWeeks(1)
                                            .with(WeekFields.of(Locale.getDefault()).dayOfWeek(), 1)
                                            .atStartOfDay(),
                             () -> LocalDate.now()
                                            .minusWeeks(1)
                                            .with(WeekFields.of(Locale.getDefault()).dayOfWeek(), 7)
                                            .atTime(23, 59));
    }

    /**
     * Creates a date range filtering on the current month.
     *
     * @return a date range for the given interval
     */
    public static DateRange thisMonth() {
        return new DateRange("thisMonth",
                             NLS.get("DateRange.thisMonth"),
                             () -> LocalDate.now().withDayOfMonth(1).atStartOfDay(),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on the last month.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastMonth() {
        return new DateRange("lastMonth",
                             NLS.get("DateRange.lastMonth"),
                             () -> LocalDate.now().minusMonths(1).withDayOfMonth(1).atStartOfDay(),
                             () -> LocalDate.now().withDayOfMonth(1).minusDays(1).atTime(23, 59));
    }

    /**
     * Creates a date range filtering on the last N months.
     * <p>
     * This will start at the first day of the selected month (current month - N) and
     * end at the day before the first of this month.
     *
     * @param months the number of months to filter.
     * @return a date range for the given interval
     */
    public static DateRange lastMonths(int months) {
        return new DateRange("lastMonths" + months,
                             NLS.fmtr("DateRange.lastMonths").set("months", months).format(),
                             () -> LocalDate.now().minusMonths(months).withDayOfMonth(1).atStartOfDay(),
                             () -> LocalDate.now().withDayOfMonth(1).minusDays(1).atTime(23, 59));
    }

    /**
     * Creates a date range filtering on the current year.
     *
     * @return a date range for the given interval
     */
    public static DateRange thisYear() {
        return new DateRange("thisYear",
                             NLS.get("DateRange.thisYear"),
                             () -> LocalDate.now().withDayOfYear(1).atStartOfDay(),
                             LocalDateTime::now);
    }

    /**
     * Creates a date range filtering on the last year.
     *
     * @return a date range for the given interval
     */
    public static DateRange lastYear() {
        return new DateRange("lastYear",
                             NLS.get("DateRange.lastYear"),
                             () -> LocalDate.now().minusYears(1).withDayOfYear(1).atStartOfDay(),
                             () -> LocalDate.now().withDayOfYear(1).minusDays(1).atStartOfDay());
    }

    /**
     * Creates a date range filtering on everything before this year.
     *
     * @return a date range for the given interval
     */
    public static DateRange beforeThisYear() {
        return new DateRange("beforeThisYear",
                             NLS.get("DateRange.beforeThisYear"),
                             () -> LocalDate.of(1900, 01, 01).atStartOfDay(),
                             () -> LocalDate.now().withDayOfYear(1).atStartOfDay());
    }

    /**
     * Creates a date range filtering on everything before the last year.
     *
     * @return a date range for the given interval
     */
    public static DateRange beforeLastYear() {
        return new DateRange("beforeLastYear",
                             NLS.get("DateRange.beforeLastYear"),
                             () -> LocalDate.of(1900, 01, 01).atStartOfDay(),
                             () -> LocalDate.now().minusYears(1).withDayOfYear(1).atStartOfDay());
    }

    /**
     * Returns the key to identify this date range.
     * <p>
     * This is e.g. used to populate facet filters based on results.
     *
     * @return the key to identify this range
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the start of the date range as {@link LocalDateTime}.
     * <p>
     * Note that when filtering on {@link sirius.db.mixing.properties.LocalDateProperty date fields}
     * this must be converted using {@link LocalDateTime#toLocalDate()} as the encoding of those two
     * is entirely different (see {@link sirius.db.jdbc.Databases#convertValue(Object))}.
     *
     * @return the start of the date range
     */
    public LocalDateTime getFrom() {
        return fromSupplier != null ? fromSupplier.get() : null;
    }

    /**
     * Returns the end of the date range as {@link LocalDateTime}.
     * <p>
     * Note that when filtering on {@link sirius.db.mixing.properties.LocalDateProperty date fields}
     * this must be converted using {@link LocalDateTime#toLocalDate()} as the encoding of those two
     * is entirely different (see {@link sirius.db.jdbc.Databases#convertValue(Object))}.
     *
     * @return the end of the date range
     */
    public LocalDateTime getUntil() {
        return untilSupplier != null ? untilSupplier.get() : null;
    }

    /**
     * Applies this date range to the given query in the given field.
     *
     * @param <C>          the constraint type to generate
     * @param field        the field to filter on
     * @param useLocalDate determines if a {@link LocalDate} instead of a {@link LocalDateTime} should be applied.
     *                     This might be crucial as a <tt>LocalDateTime</tt> is encoded entirely differently
     *                     (see {@link sirius.db.jdbc.Databases#convertValue(Object))}.
     * @param query        the query to expand
     */
    public <C extends Constraint> void applyTo(String field, boolean useLocalDate, Query<?, ?, C> query) {
        List<C> constraints = new ArrayList<>(2);
        if (fromSupplier != null) {
            LocalDateTime from = fromSupplier.get();
            constraints.add(query.filters()
                                 .gte(Mapping.named(field), useLocalDate ? from.toLocalDate() : from));
        }
        if (untilSupplier != null) {
            LocalDateTime until = untilSupplier.get();
            constraints.add(query.filters()
                                 .lte(Mapping.named(field),
                                      useLocalDate ? until.toLocalDate() : until));
        }
        query.where(query.filters().and(constraints));
    }

    @Override
    public String toString() {
        return name;
    }
}
