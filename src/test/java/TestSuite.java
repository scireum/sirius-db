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
import sirius.kernel.Scope;

@RunWith(ScenarioSuite.class)
@SuiteClasses({"**/*Test.class", "**/*Spec.class"})
@Scenario(file = "test-mariadb-latest.conf",
        includes = "sirius\\.db\\.jdbc.*",
        excludes = "sirius\\.db\\.jdbc.clickhouse.*",
        scope = Scope.SCOPE_NIGHTLY)
@Scenario(file = "test-percona-latest.conf",
        includes = "sirius\\.db\\.jdbc.*",
        excludes = "sirius\\.db\\.jdbc.clickhouse.*",
        scope = Scope.SCOPE_NIGHTLY)
@Scenario(file = "test-redis-latest.conf", includes = "sirius\\.db\\.redis.*", scope = Scope.SCOPE_NIGHTLY)
@Scenario(file = "test-mongo-latest.conf", includes = "sirius\\.db\\.mongo.*", scope = Scope.SCOPE_NIGHTLY)
@Scenario(file = "test-es-sellsite.conf", includes = "sirius\\.db\\.es.*", scope = Scope.SCOPE_NIGHTLY)
public class TestSuite {

}
