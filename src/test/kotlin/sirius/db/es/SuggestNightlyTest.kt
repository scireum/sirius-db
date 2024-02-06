/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es


import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.constraints.BoolQueryBuilder
import sirius.db.es.suggest.SuggesterBuilder
import sirius.kernel.SiriusExtension
import sirius.kernel.Tags
import sirius.kernel.commons.Json
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
@Tag(Tags.NIGHTLY)
class SuggestNightlyTest {
    @Test
    fun `term suggest works`() {
        // Add enough data to make sure elastic returns all expected suggestions
        for (i in 0..99) {
            val entity1 = SuggestTestEntity()
            entity1.content = "HSS drill bit"
            elastic.update(entity1)
            val entity2 = SuggestTestEntity()
            entity2.content = "Salmon with dill"
            elastic.update(entity2)
        }
        elastic.refresh(SuggestTestEntity::class.java)
        val suggestParts = elastic.suggest(SuggestTestEntity::class.java)
                .withTermSuggester("test", SuggestTestEntity.CONTENT, "HSS dril bitt").execute().getSuggestions("test")

        assertEquals(3, suggestParts.size)
        assertEquals(0, suggestParts[0].termSuggestions.size)
        assertEquals(2, suggestParts[1].termSuggestions.size)
        assertEquals(1, suggestParts[2].termSuggestions.size)

        assertTrue {
            suggestParts[1].termSuggestions.stream()
                    .anyMatch { option -> option.text.equals("drill", ignoreCase = true) }
        }
        assertTrue {
            suggestParts[1].termSuggestions.stream()
                    .anyMatch { option -> option.text.equals("dill", ignoreCase = true) }
        }
        assertTrue { suggestParts[2].termSuggestions[0].text.equals("bit", ignoreCase = true) }
    }

    @Test
    fun `phrase suggest works`() {
        // Add enough data to make sure elastic returns all expected suggestions
        for (i in 0..99) {
            val entity1 = SuggestTestEntity()
            entity1.content = "HSS drill bit"
            elastic.update(entity1)
            val entity2 = SuggestTestEntity()
            entity2.content = "Salmon with dill"
            elastic.update(entity2)
        }
        elastic.refresh(SuggestTestEntity::class.java)
        val suggestOptions = elastic.suggest(SuggestTestEntity::class.java).withSuggester(
                SuggesterBuilder(SuggesterBuilder.TYPE_PHRASE, "test").on(SuggestTestEntity.CONTENT)
                        .forText("dril potatoes")
        ).execute().getSingleTermSuggestions("test")

        assertEquals(2, suggestOptions.size)
        assertTrue {
            suggestOptions.stream().anyMatch { option -> option.text.equals("drill potatoes", ignoreCase = true) }
        }
        assertTrue {
            suggestOptions.stream().anyMatch { option -> option.text.equals("dill potatoes", ignoreCase = true) }
        }
    }

    @Test
    fun `phrase suggest with a collate query works`() {
        // Add enough data as the collate query only runs on one shard
        for (i in 0..99) {
            val suggestTestEntity1 = SuggestTestEntity()
            suggestTestEntity1.content = "Salsa Tomaten"
            suggestTestEntity1.shop = 1
            elastic.update(suggestTestEntity1)
            val suggestTestEntity2 = SuggestTestEntity()
            suggestTestEntity2.content = "Kartoffeln mit Salz"
            suggestTestEntity2.shop = 1
            elastic.update(suggestTestEntity2)
            val suggestTestEntity3 = SuggestTestEntity()
            suggestTestEntity3.content = "Kartoffel Sale"
            suggestTestEntity3.shop = 2
            elastic.update(suggestTestEntity3)
        }
        val matchPhrase = Json.createObject().set<ObjectNode>(
                "match",
                Json.createObject().set(
                        SuggestTestEntity.CONTENT.name,
                        Json.createObject().put("query", "{{suggestion}}").put("operator", "and")
                )
        )
        val matchShop = Elastic.FILTERS.eq(SuggestTestEntity.SHOP, 1)
        elastic.refresh(SuggestTestEntity::class.java)
        val suggestOptions = elastic.suggest(SuggestTestEntity::class.java).withSuggester(
                SuggesterBuilder(SuggesterBuilder.TYPE_PHRASE, "test").on(SuggestTestEntity.CONTENT)
                        .forText("Sals Kartoffeln")
                        .collate(BoolQueryBuilder().must(matchPhrase).must(matchShop).build(), true)
        ).execute().getSingleTermSuggestions("test")

        assertEquals(3, suggestOptions.size)
        assertTrue {
            suggestOptions.stream().anyMatch { option ->
                option.text.equals(
                        "Salsa Kartoffeln",
                        ignoreCase = true
                ) && !option.isCollateMatch
            }
        }
        assertTrue {
            suggestOptions.stream().anyMatch { option ->
                option.text.equals(
                        "Sale Kartoffeln",
                        ignoreCase = true
                ) && !option.isCollateMatch
            }
        }
        assertTrue {
            suggestOptions.stream().anyMatch { option ->
                option.text.equals(
                        "Salz Kartoffeln",
                        ignoreCase = true
                ) && option.isCollateMatch
            }
        }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            elastic.getReadyFuture().await(Duration.ofSeconds(60))
        }
    }
}
