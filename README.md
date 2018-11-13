# kbeam
[![GitHub license](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg?style=flat)](http://www.apache.org/licenses/LICENSE-2.0)

KBeam is a library to help write Apache Beam pipelines with less ceremony and verbosity compared to Java while keeping or improving the strong typing guarentees absent from python pipeline builders.

Comparable to [Euphoria](https://beam.apache.org/roadmap/euphoria/) but for Kotlin


## Features Overview

* Fully idiomatic Trivial map/flatMap/filter   
* Multiple output helpers for common cases
* Codecs for Kotlin data classes, common tuples (Pair,Triple)
* IO DSL

## Quick example

The following pipeline in Java: 
```java


```

Converted to KBeam:
```kotlin


```
## Table of contents
* [Features Overview](#features-overview)
* [Quick example](#quick-example)
* [Current status](#current-project-status)

## Setup

*TODO*: 
* Create and deploy maven artefact to central
* Create a mvn template project and gradle template

## Current Project Status

- [x] Type safe custom configuration pipeline creation
- [x] Basic lambda map/filter/flatMap
- [x] Generic ParDo with Side Inputs
- [x] Multiple assignment output with TupleTag hiding for dual and triple outputs in ParDo
- [x] Simple filter based dual output splitter
- [ ] Multiple assignment output with TupleTag hiding for 4 to 8 outputs in ParDo
- [ ] Partition helpers for 3 to 8 outputs
- [ ] CoGroupByKey multiple assignment output with TupleTag hiding
- [ ] Codec builders with kotlinx.serialization 
- [ ] TextIO DSL helpers
- [ ] Kafka DSL helpers
- [ ] Pub/Sub DSL helpers
- [ ] ElasticSearch DSL helpers
- [ ] BigQuery DSL helpers
- [ ] BigTable DSL helpers
- [ ] MongoDb DSL helpers
