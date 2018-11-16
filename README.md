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
([Full example](https://github.com/pujo-j/kbeam/blob/master/src/test/java/com/sfeir/open/kbeam/SamplePipeline.java)) 
```java
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs("--test=toto").withValidation().as(MyOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Load mapping of country code to country
        PCollection<String> countryCodes = pipeline.apply("Read Country File", TextIO.read().from("src/test/resources/country_codes.jsonl"));
        PCollection<KV<String, String>> countryCodesKV = countryCodes.apply("Parse", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())).via(
                (String line) -> {
                    try {
                        JsonNode jsonLine = json.get().readTree(line);
                        return KV.of(jsonLine.get("Code").getTextValue(), jsonLine.get("Name").getTextValue());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        ));

        PCollectionView<Map<String, String>> countryCodesView = countryCodesKV.apply(View.asMap());


        // Load list of lines
        PCollection<String> lines = pipeline.apply("Read lines", TextIO.read().from("src/test/resources/test.csv"));
        PCollection<Entry> entries = lines.apply("Map to entries", ParDo.of(new DoFn<String, Entry>() {
            @ProcessElement
            public void process(ProcessContext context) {
                String[] words = context.element().split(",");
                if (words.length == 3) {
                    context.output(new Entry(words[0], words[1], Double.parseDouble(words[2])));
                }
            }
        }));

        // Fill in country names
        PCollection<Entry> entries2 = entries.apply("Enrich with country name", ParDo.of(new DoFn<Entry, Entry>() {
            @ProcessElement
            public void process(ProcessContext context) {
                String code = context.element().getCountryCode();
                String name = context.sideInput(countryCodesView).getOrDefault(code, "unknown");
                context.output(new Entry(context.element().getName(), context.element().getCountryCode(), context.element().getDoubleValue(), name));
            }
        }).withSideInputs(countryCodesView));

        // split in two collections
        TupleTag<Entry> positiveEntries = new TupleTag<Entry>() {
        };
        TupleTag<Entry> negativeEntries = new TupleTag<Entry>() {
        };

        PCollectionTuple splitResult = entries2.apply("Split positives and negatives", ParDo.of(new DoFn<Entry, Entry>() {
            @ProcessElement
            public void process(ProcessContext context) {
                if (context.element().getDoubleValue() >= 0) {
                    context.output(positiveEntries, context.element());
                } else {
                    context.output(negativeEntries, context.element());
                }
            }
        }).withOutputTags(positiveEntries, TupleTagList.of(negativeEntries)));

        // this is an example so let's just print the results

        splitResult.get(positiveEntries).apply(ParDo.of(new DoFn<Entry, Void>() {
            @ProcessElement
            public void process(ProcessContext context) {
                System.out.println("Positive :" + context.element().toString());
            }
        }));

        splitResult.get(negativeEntries).apply(ParDo.of(new DoFn<Entry, Void>() {
            @ProcessElement
            public void process(ProcessContext context) {
                System.out.println("Negative:" + context.element().toString());
            }
        }));

        pipeline.run(options).waitUntilFinish();

```

Converted to KBeam:
([Full example](https://github.com/pujo-j/kbeam/blob/master/src/test/kotlin/com/sfeir/open/kbeam/SamplePipelineKotlin.kt))
```kotlin
        val (pipeline, options) = PipeBuilder.create<KMyOptions>(arrayOf("--test=toto"))
        println("$pipeline, $options")

        val countryCodes = pipeline
                .readTextFile(name = "Read Country File", path = "src/test/resources/country_codes.jsonl")
                .map {
                    val line = Json.mapper.readTree(it)
                    KV.of(line["Code"].textValue, line["Name"].textValue)
                }.toMap()

        val test = pipeline.readTextFile(name = "Read Lines", path = "src/test/resources/test.csv")
                .filter { it.isNotEmpty() }
                .map(name = "Map to entries") {
                    val words = it.split(",")
                    KEntry(words[0], words[1], words[2].toDouble())
                }.parDo<KEntry, KEntry>(
                        name = "Join with countries",
                        sideInputs = listOf(countryCodes)) {
                    val countryName = sideInputs[countryCodes][element.countryCode] ?: "unknown"
                    output(element.copy(countryName = countryName))
                }

        val (positives, negatives) = test.split {
            println(it)
            it.doubleValue >= 0
        }

        positives.parDo<KEntry, Void> {
            println("Positive: $element")
        }


        negatives.parDo<KEntry, Void> {
            println("Negative: $element")
        }

        pipeline.run().waitUntilFinish()
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
- [x] Multiple assignment output with TupleTag hiding for 4 to 8 outputs in ParDo
- [ ] Partition helpers for 3 to 8 outputs
- [x] CoGroupByKey multiple assignment output with TupleTag hiding
- [ ] Codec builders with kotlinx.serialization 
- [ ] Codec builders with kryo
- [ ] TextIO DSL helpers
- [ ] Kafka DSL helpers
- [ ] Pub/Sub DSL helpers
- [ ] ElasticSearch DSL helpers
- [ ] BigQuery DSL helpers
- [ ] BigTable DSL helpers
- [ ] MongoDb DSL helpers
