package com.sfeir.open.kbeam

import org.apache.avro.data.Json
import org.apache.avro.util.internal.JacksonUtils
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.junit.jupiter.api.Test
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.codehaus.jackson.map.ObjectMapper


interface MyOptions : PipelineOptions {
    @Description("Custom Option")
    @Default.String("test")
    fun getTest(): String;

    fun setTest(value: String): Unit
}

class TestStandardPipeline {

    @DefaultCoder(AvroCoder::class)
    data class Entry(
            val name: String = "",
            val countryCode: String = "",
            val doubleValue: Double = 0.0,
            val countryName: String = "unknown")


    @Test
    fun runTest() {
        PipelineOptionsFactory.register(MyOptions::class.java)
        val options = PipelineOptionsFactory.fromArgs("--test=toto").withValidation().`as`(MyOptions::class.java)
        val pipeline = Pipeline.create(options)
        println("$pipeline, $options")
        val lines = pipeline.apply("Read Lines", TextIO.read().from("src/test/resources/test.csv"))

        val entries = lines.apply("Map to entries", ParDo.of(object : DoFn<String, Entry>() {
            @ProcessElement
            fun process(context: ProcessContext) {
                val words = context.element().split(",")
                if (words.size == 3) {
                    context.output(Entry(words[0], words[1], words[2].toDouble()))
                }
            }
        }))

        val positiveEntries = entries.apply("Filter only positive", ParDo.of(object : DoFn<Entry, Entry>() {
            @ProcessElement
            fun process(context: ProcessContext) {
                if (context.element().doubleValue > 0) {
                    context.output(context.element())
                }
            }
        }))

        val sideLines = pipeline.apply(Create.of(listOf("Side1", "Side2", "Side3")))
        val sideLinesView = sideLines.apply(View.asIterable())

        positiveEntries.apply("Print lines", ParDo.of(object : DoFn<Entry, String>() {
            @ProcessElement
            fun process(context: ProcessContext) {
                val out = "${context.element()} : ${context.sideInput(sideLinesView).first()}"
                println(out)
            }
        }).withSideInputs(sideLinesView))

        pipeline.run().waitUntilFinish()
    }
}

class TestDSLPipeline {

    object Json {
        val mapper: ObjectMapper by lazy<ObjectMapper> {
            ObjectMapper()
        }
    }

    @Test
    fun runTest() {
        val (pipeline, options) = PipeBuilder.create<MyOptions>(arrayOf("--test=toto"))
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
                    TestStandardPipeline.Entry(words[0], words[1], words[2].toDouble())
                }.parDo<TestStandardPipeline.Entry, TestStandardPipeline.Entry>(
                        name = "Join with countries",
                        sideInputs = listOf(countryCodes)) {
                    val countryName = sideInputs[countryCodes][element.countryCode] ?: "unknown"
                    output(element.copy(countryName = countryName))
                }

        val (positives, negatives) = test.split {
            println(it)
            it.doubleValue >= 0
        }

        positives.parDo<TestStandardPipeline.Entry, Void> {
            println("Positive: $element")
        }


        negatives.parDo<TestStandardPipeline.Entry, Void> {
            println("Negative: $element")
        }

        pipeline.run().waitUntilFinish()
    }
}