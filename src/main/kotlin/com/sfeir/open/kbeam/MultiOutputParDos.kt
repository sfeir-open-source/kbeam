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

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.*
import org.joda.time.Instant


class DoFnContextWrapper_2Outputs<I, O1, O2>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>) : DoFnContextWrapper<I, O1>(processContext) {
    fun output2(item: O2) {
        outputTagged(tag2, item)
    }

    fun output2Timestamped(item: O2, timestamp: Instant) {
        outputTaggedTimestamped(tag2, item, timestamp)
    }
}

/**
 * Generic parDo extension method for two outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified OutputType, reified OutputType2> PCollection<InputType>.parDo2(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper_2Outputs<InputType, OutputType, OutputType2>.() -> Unit): Pair<PCollection<OutputType>, PCollection<OutputType2>> {
    val output1Tag = object : TupleTag<OutputType>() {}
    val output2Tag = object : TupleTag<OutputType2>() {}
    val tagList = TupleTagList.of(listOf(output2Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, OutputType>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper_2Outputs(context, output2Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return Pair(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag])
}

class DoFnContextWrapper_3Outputs<I, O1, O2, O3>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>, private val tag3: TupleTag<O3>) : DoFnContextWrapper<I, O1>(processContext) {
    fun output2(item: O2) {
        outputTagged(tag2, item)
    }

    fun output2Timestamped(item: O2, timestamp: Instant) {
        outputTaggedTimestamped(tag2, item, timestamp)
    }

    fun output3(item: O3) {
        outputTagged(tag3, item)
    }

    fun output3Timestamped(item: O3, timestamp: Instant) {
        outputTaggedTimestamped(tag3, item, timestamp)
    }
}

/**
 * Generic parDo extension method for three outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output*, *output2* and *output3*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified OutputType, reified OutputType2, reified OutputType3> PCollection<InputType>.parDo3(name: String = "ParDo",
                                                                                                                   sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                                                   , crossinline function: DoFnContextWrapper_3Outputs<InputType, OutputType, OutputType2, OutputType3>.() -> Unit):
        Triple<PCollection<OutputType>, PCollection<OutputType2>, PCollection<OutputType3>> {
    val output1Tag = object : TupleTag<OutputType>() {}
    val output2Tag = object : TupleTag<OutputType2>() {}
    val output3Tag = object : TupleTag<OutputType3>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, OutputType>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper_3Outputs(context, output2Tag, output3Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return Triple(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag])
}


/**
 * Generic parDo extension method for multiOutputs
 *
 * The executed lambda has access to an implicit process context as *this*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 * @param outputs The "other" outputs
 */
inline fun <InputType, OutputType> PCollection<InputType>.parDoMultiOutputs(name: String = "ParDo",
                                                                            sideInputs: List<PCollectionView<*>> = emptyList(),
                                                                            outputs: List<TupleTag<*>> = emptyList(),
                                                                            crossinline function: DoFnContext<InputType, OutputType>.() -> Unit): PCollectionTuple {
    val tagList = TupleTagList.of(outputs)
    return this.apply(name, ParDo.of(object : DoFn<InputType, OutputType>() {
        @ProcessElement
        fun processElement(processContext: ProcessContext) {
            DoFnContextWrapper(processContext).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(TupleTag<OutputType>(), tagList))
}
