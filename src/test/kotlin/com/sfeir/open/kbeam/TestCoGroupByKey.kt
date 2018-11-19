/*
 * Copyright 2018 SFEIR S.A.S.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.sfeir.open.kbeam

import com.sfeir.open.kbeam.io.writeText
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.junit.jupiter.api.Test

class TestCoGroupByKey {
    @Test
    fun runTest() {
        val (pipeline, options) = PipeBuilder.create<PipelineOptions>(arrayOf())

        val list1 = (1..20).map { KV.of("Key_${it % 10})", it) }
        val list2 = (0..9).flatMap {
            val k = it
            ('a'..'z').map { KV.of("Key_$k)", "$it") }
        }
        val plist1 = pipeline.apply("Create List1", Create.of(list1))
        val plist2 = pipeline.apply("Create List2", Create.of(list2))

        val group = pipeline.coGroupByKey(plist1, plist2) { key, left, right ->
            listOf("$key : $left $right")
        }

        group.parDo<String, Void> {
            println(element)
        }

        group.writeText {
            path = "test"
            compression = Compression.GZIP
            numShards = 1
            header = "#Header"
            footer = "#Footer"
        }
        pipeline.run().waitUntilFinish()
    }
}