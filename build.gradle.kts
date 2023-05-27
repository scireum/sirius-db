/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

plugins {
    id("java-library")
    id("com.scireum.sirius-parent") version "11.0.5"
    id("org.sonarqube") version "3.4.0.2513"
    id("io.github.joselion.pretty-jupiter") version "2.2.0"
}

apply(plugin = "com.scireum.sirius-parent")

dependencies {
    api("com.scireum:sirius-kernel:${property("sirius-kernel")}")
    testImplementation("com.scireum:sirius-kernel:${property("sirius-kernel")}") {
        artifact {
            classifier = "tests"
        }
    }

    api("org.apache.commons:commons-dbcp2:2.9.0")

    api("org.mongodb:mongodb-driver-sync:4.7.1")

    api("redis.clients:jedis:4.2.3")

    api("org.elasticsearch.client:elasticsearch-rest-client:8.4.1")
    // Required as the version brought by elasticsearch-rest-client contains security issues
    api("org.apache.httpcomponents:httpclient:4.5.13")

    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.0.6")

    testImplementation("com.clickhouse:clickhouse-jdbc:0.3.2-patch11")
    // Required as the version brought by clickhouse-jdbc contains security issues
    testImplementation("com.fasterxml.jackson.core:jackson-databind")
}

sonarqube {
    properties {
        property("sonar.sourceEncoding", "UTF-8")
    }
}
