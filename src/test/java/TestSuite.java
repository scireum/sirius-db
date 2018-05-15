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
@Scenario(file = "test-mariadb-latest.conf", filter = "sirius\\.db\\.(jdbc|mixing).*")
@Scenario(file = "test-percona-latest.conf", filter = "sirius\\.db\\.(jdbc|mixing).*")
@Scenario(file = "test-postgres-latest.conf", filter = "sirius\\.db\\.(jdbc).*")
@Scenario(file = "test-redis-latest.conf", filter = "sirius\\.db\\.redis.*")
@Scenario(file = "test-mongo-latest.conf", filter = "sirius\\.db\\.mongo.*")
public class TestSuite {

}
