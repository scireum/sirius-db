/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

import com.googlecode.junittoolbox.SuiteClasses;
import org.junit.runner.RunWith;
import sirius.kernel.Scenario;
import sirius.kernel.ScenarioSuite;

@RunWith(ScenarioSuite.class)
@SuiteClasses({"**/*Test.class", "**/*Spec.class"})
@Scenario(file = "test-mariadb-latest.conf",
        includes = "sirius\\.db\\.jdbc.*",
        excludes = "sirius\\.db\\.jdbc.clickhouse.*")
@Scenario(file = "test-percona-latest.conf",
        includes = "sirius\\.db\\.jdbc.*",
        excludes = "sirius\\.db\\.jdbc.clickhouse.*")
//@Scenario(file = "test-postgres-latest.conf", filter = "sirius\\.db\\.(jdbc).*")
@Scenario(file = "test-redis-latest.conf", includes = "sirius\\.db\\.redis.*")
@Scenario(file = "test-mongo-latest.conf", includes = "sirius\\.db\\.mongo.*")
@Scenario(file = "test-elasticsearch-latest.conf", includes = "sirius\\.db\\.es.*")
public class TestSuite {

}
