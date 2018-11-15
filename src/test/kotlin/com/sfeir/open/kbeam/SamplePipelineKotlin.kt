/*
 *    Copyright 2018 SFEIR S.A.S.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.sfeir.open.kbeam

import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.values.KV
import org.codehaus.jackson.map.ObjectMapper
import org.junit.jupiter.api.Test


interface KMyOptions : PipelineOptions {
    @Description("Custom Option")
    @Default.String("test")
    fun getTest(): String

    fun setTest(value: String)
}

@DefaultCoder(AvroCoder::class)
data class KEntry(
        val name: String = "",
        val countryCode: String = "",
        val doubleValue: Double = 0.0,
        val countryName: String = "unknown")


class TestDSLPipeline {

    object Json {
        val mapper: ObjectMapper by lazy<ObjectMapper> {
            ObjectMapper()
        }
    }

    @Test
    fun runTest() {
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
    }
}