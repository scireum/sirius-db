# sirius-db
![sirius](https://raw.githubusercontent.com/scireum/sirius-kernel/main/docs/sirius.jpg)
[![Build Status](https://drone.scireum.com/api/badges/scireum/sirius-db/status.svg?ref=refs/heads/main)](https://drone.scireum.com/scireum/sirius-db)

Welcome to the **database module** of the SIRIUS OpenSource framework created by [scireum GmbH](https://www.scireum.de). 
To learn more about what SIRIUS is please refer to documentation of the [kernel module](https://github.com/scireum/sirius-kernel).

# SIRIUS Database Module

Provides access to popular databases like **JDBC/SQL, MongoDB, Redis or Elasticsearch**.

For each database a *low level interface* is provided which combines configuration, monitoring,
metrics and debugging with the best performance possible.

Additionally a mapping layer is provided for JDBC, MongoDB and Elasticsearch, which is called 
[Mixing](src/main/java/sirius/db/mixing).

If you want to use **sirius-db** in a web environment (in conjunction with **sirius-web**) have a look
at [sirius-biz](https://github.com/scireum/sirius-biz) which provides a lot of helpers for that scenario.

## Features

* Database independent mapping framework which can be extended by user code
* [Mixins](src/main/java/sirius/db/mixing) which can extend existing model classes and be enabled within customizations
* Wrappers for efficient calls into JDBC databases, Redis and MongoDB with automatic connection management
* Automatic schema and index generation for all managed databases - note that **sirius-biz** provides a
  [UI](https://github.com/scireum/sirius-biz/tree/main/src/main/java/sirius/biz/jdbc) for this
* Metrics and Monitoring of all database operations - note that **sirius-web** provides a 
  [UI](https://github.com/scireum/sirius-web/tree/main/src/main/java/sirius/web/health) to display these stats 
  and timings
* Automatic reporting for slow or inefficient database operations 

## Important files of this module: 

* [Default configuration](src/main/resources/component-060-db.conf)
* [Maven setup](pom.xml)

## Frameworks

* [Mixing](src/main/java/sirius/db/mixing)\
Provides a database independent mapping framework which can be extended by user code.
* [JDBC](src/main/java/sirius/db/jdbc)\
Provides a both, a low-level interface which adds configuration- and resource management to JDBC while still
delivering its raw performance. Also a high-level mapper based on **Mixing** is provided.
* [ES](src/main/java/sirius/db/es)\
Provides an HTTP based client for Elasticsearch which utilizes **Mixing** to provide a high-level API.
* [Mongo](src/main/java/sirius/db/mongo)\
Provides a both, a low-level interface which adds configuration- and resource management to the plain MongoDB driver while still
delivering its raw performance. Also a high-level mapper based on **Mixing** is provided.
* [Redis](src/main/java/sirius/db/redis)\
Contains a helper framework which provides configuration- and resource management when talking to one or more Redis servers.
