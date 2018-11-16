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

package com.sfeir.open.kbeam.generator

import java.io.File
import java.io.IOException

object ParDosGenerator {
    val header = """/*
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

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.*
import org.joda.time.Instant
"""

    fun genParDo(n: Int): String {
        val outputTypeList = (1..n).joinToString(",") { "O$it" }
        val reifiedOutputTypeList = (1..n).joinToString(",") { "reified O$it" }
        return """

data class DoFn${n}Outputs<$outputTypeList>(${(1..n).joinToString(",") { "val output$it: PCollection<O$it>" }})

class DoFnContextWrapper${n}Outputs<I, $outputTypeList>(processContext: DoFn<I, O1>.ProcessContext, ${(2..n).joinToString(",") { "private val tag$it: TupleTag<O$it>" }}) : DoFnContextWrapper<I, O1>(processContext) {
${(2..n).joinToString("\n") {
            """
     fun output$it(item: O$it) {
        outputTagged(tag$it, item)
    }

    fun output${it}Timestamped(item: O$it, timestamp: Instant) {
        outputTaggedTimestamped(tag$it, item, timestamp)
    }

"""
        }}
}

/**
 * Generic parDo extension method for $n outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, $reifiedOutputTypeList> PCollection<InputType>.parDo$n(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper${n}Outputs<InputType, $outputTypeList>.() -> Unit): DoFn${n}Outputs<$outputTypeList> {
${(1..n).joinToString("\n") {
            "val output${it}Tag = object : TupleTag<O$it>() {}"
        }}
    val tagList = TupleTagList.of(listOf(${(2..n).joinToString { "output${it}Tag" }}))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper${n}Outputs(context, ${(2..n).joinToString { "output${it}Tag" }}).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn${n}Outputs(${(1..n).joinToString { "pCollectionTuple[output${it}Tag]" }})
}
"""
    }

    fun getParDos(outputDir: String): Unit {
        val output = File(outputDir + "/com/sfeir/open/kbeam/MultiOutputParDos.kt")
        if (!output.parentFile.exists()) {
            output.parentFile.mkdirs()
        } else {
            if (!output.parentFile.isDirectory) {
                throw IOException("Output dir is a file, should be a directory")
            }
        }
        output.delete()
        output.createNewFile()
        with(output.outputStream().bufferedWriter(Charsets.UTF_8)) {
            write(header)
            (2..8).forEach {
                write(genParDo(it))
            }
            flush()
        }
    }
}
