---
layout: default
title: Rationale
---

[Apache Beam](https://beam.apache.org/) brings a great model for both both batch and streaming data transformation and the Java model and runtimes are mature... **But**

Java Pipeline definitions are overly verbose, the cost of full type safety in a language that does not have reified types and is already known for verbosity.

KBeam's goal is to keep the full typing goodness of the Java model, without the epic eye-sore of the Java model.

Going from this:
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

To that:
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

Is a *not so extreme* example  