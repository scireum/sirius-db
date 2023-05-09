/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import com.alibaba.fastjson.JSONObject
import org.junit.jupiter.api.Tag
import sirius.db.es.constraints.BoolQueryBuilder

import sirius.db.es.suggest.SuggesterBuilder
import sirius.kernel.BaseSpecification
import sirius.kernel.Tags
import sirius.kernel.di.std.Part

import java.time.Duration
import java.util.function.Predicate

@Tag(Tags.NIGHTLY)
class SuggestSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "term suggest works"() {
        when:
        // Add enough data to make sure elastic returns all expected suggestions
        for (int i = 0; i < 100; i++) {
            def entity1 = new SuggestTestEntity()
            entity1.setContent("HSS drill bit")
            elastic.update(entity1)

            def entity2 = new SuggestTestEntity()
            entity2.setContent("Salmon with dill")
            elastic.update(entity2)
        }
        and:
        elastic.refresh(SuggestTestEntity.class)
        def suggestParts = elastic.suggest(SuggestTestEntity.class)
                                  .withTermSuggester("test", SuggestTestEntity.CONTENT, "HSS dril bitt")
                                  .execute()
                                  .getSuggestions("test")
        then:
        suggestParts.size() == 3
        suggestParts.get(0).getTermSuggestions().size() == 0
        suggestParts.get(1).getTermSuggestions().size() == 2
        suggestParts.get(2).getTermSuggestions().size() == 1
        and:
        suggestParts
                .get(1)
                .getTermSuggestions()
                .stream()
                .anyMatch({ option -> option.getText().equalsIgnoreCase("drill") } as Predicate)
        suggestParts
                .get(1)
                .getTermSuggestions()
                .stream()
                .anyMatch({ option -> option.getText().equalsIgnoreCase("dill") } as Predicate)
        suggestParts.get(2).getTermSuggestions().get(0).getText().equalsIgnoreCase("bit")
    }

    def "phrase suggest works"() {
        when:
        // Add enough data to make sure elastic returns all expected suggestions
        for (int i = 0; i < 100; i++) {
            def entity1 = new SuggestTestEntity()
            entity1.setContent("HSS drill bit")
            elastic.update(entity1)

            def entity2 = new SuggestTestEntity()
            entity2.setContent("Salmon with dill")
            elastic.update(entity2)
        }
        and:
        elastic.refresh(SuggestTestEntity.class)
        def suggestOptions = elastic.suggest(SuggestTestEntity.class)
                                    .withSuggester(new SuggesterBuilder(SuggesterBuilder.TYPE_PHRASE, "test")
                                                           .on(SuggestTestEntity.CONTENT).forText("dril potatoes"))
                                    .execute()
                                    .getSingleTermSuggestions("test")
        then:
        suggestOptions.size() == 2
        and:
        suggestOptions.stream().anyMatch({ option -> option.getText().equalsIgnoreCase("drill potatoes") } as Predicate)
        suggestOptions.stream().anyMatch({ option -> option.getText().equalsIgnoreCase("dill potatoes") } as Predicate)
    }

    def "phrase suggest with a collate query works"() {
        when:
        // Add enough data as the collate query only runs on one shard
        for (int i = 0; i < 100; i++) {
            def entity1 = new SuggestTestEntity()
            entity1.setContent("Salsa Tomaten")
            entity1.setShop(1)
            elastic.update(entity1)

            def entity2 = new SuggestTestEntity()
            entity2.setContent("Kartoffeln mit Salz")
            entity2.setShop(1)
            elastic.update(entity2)

            def entity3 = new SuggestTestEntity()
            entity3.setContent("Kartoffel Sale")
            entity3.setShop(2)
            elastic.update(entity3)
        }
        and:
        def matchPhrase = new JSONObject().fluentPut("match",
                                                     new JSONObject().fluentPut(SuggestTestEntity.CONTENT.getName(),
                                                                                new JSONObject().fluentPut("query",
                                                                                                           "{{suggestion}}")
                                                                                                .fluentPut("operator",
                                                                                                           "and")))

        def matchShop = Elastic.FILTERS.eq(SuggestTestEntity.SHOP, 1)


        and:
        elastic.refresh(SuggestTestEntity.class)
        def suggestOptions = elastic.suggest(SuggestTestEntity.class)
                                    .withSuggester(new SuggesterBuilder(SuggesterBuilder.TYPE_PHRASE, "test")
                                                           .on(SuggestTestEntity.CONTENT).forText("Sals Kartoffeln")
                                                           .collate(new BoolQueryBuilder().must(matchPhrase)
                                                                                          .must(matchShop)
                                                                                          .build(),
                                                                    true))
                                    .execute()
                                    .getSingleTermSuggestions("test")
        then:
        suggestOptions.size() == 3
        and:
        suggestOptions.stream()
                      .anyMatch({ option -> option.getText().equalsIgnoreCase("Salsa Kartoffeln") && !option.isCollateMatch() } as Predicate)

        suggestOptions.stream()
                      .anyMatch({ option -> option.getText().equalsIgnoreCase("Sale Kartoffeln") && !option.isCollateMatch() } as Predicate)

        suggestOptions.stream()
                      .anyMatch({ option -> option.getText().equalsIgnoreCase("Salz Kartoffeln") && option.isCollateMatch() } as Predicate)
    }
}
