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

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.joda.time.Instant

/**
 * Utility class to access sideInputs as a "map like" object from a process context
 * @param context The context to wrap
 */
class SideInputs<I>(private val context: DoFnContext<I, *>) {
    /**
     * "Map like" Access to side input views
     */
    operator fun <T> get(view: PCollectionView<T>): T {
        return context.context.sideInput(view)
    }
}

/**
 * DoFn.ProcessContext interface
 *
 * Clean interface replacing the original abstract class for easier delegation and extensions
 */
interface DoFnContext<InputType, OutputType> {
    val context: DoFn<InputType, OutputType>.ProcessContext

    val options: PipelineOptions
    val element: InputType
    val sideInputs: SideInputs<InputType>
    val timestamp: Instant
    val pane: PaneInfo
    fun updateWatermark(watermark: Instant)

    fun output(item: OutputType)
    fun outputTimeStamped(item: OutputType, timestamp: Instant)
    fun <T> outputTagged(tag: TupleTag<T>, item: T)
    fun <T> outputTaggedTimestamped(tag: TupleTag<T>, item: T, timestamp: Instant)
}

open class DoFnContextWrapper<InputType, OutputType>(override val context: DoFn<InputType, OutputType>.ProcessContext) : DoFnContext<InputType, OutputType> {

    override val options: PipelineOptions
        get() = context.pipelineOptions

    override val element: InputType
        get() = context.element()

    @Suppress("LeakingThis")
    final override val sideInputs: SideInputs<InputType> = SideInputs(this)

    override val timestamp: Instant
        get() = context.timestamp()
    override val pane: PaneInfo
        get() = context.pane()

    override fun updateWatermark(watermark: Instant) {
        context.updateWatermark(watermark)
    }

    override fun output(item: OutputType) {
        context.output(item)
    }

    override fun outputTimeStamped(item: OutputType, timestamp: Instant) {
        context.outputWithTimestamp(item, timestamp)
    }

    override fun <T> outputTagged(tag: TupleTag<T>, item: T) {
        context.output(tag, item)
    }

    override fun <T> outputTaggedTimestamped(tag: TupleTag<T>, item: T, timestamp: Instant) {
        context.outputWithTimestamp(tag, item, timestamp)
    }
}

/**
 * Generic parDo extension method
 *
 * The executed lambda has access to an implicit process context as *this*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, OutputType> PCollection<InputType>.parDo(name: String = "ParDo",
                                                                sideInputs: List<PCollectionView<*>> = emptyList(),
                                                                crossinline function: DoFnContext<InputType, OutputType>.() -> Unit):
        PCollection<OutputType> {
    return this.apply(name, ParDo.of(object : DoFn<InputType, OutputType>() {
        @ProcessElement
        fun processElement(processContext: ProcessContext) {
            DoFnContextWrapper(processContext).apply(function)
        }
    }).withSideInputs(sideInputs))
}

/**
 * Filter the input PCollection by a condition
 *
 * The executed lambda has access to an implicit process context as *this* if needed
 */
inline fun <InputType> PCollection<InputType>.filter(name: String = "filter", crossinline function: (InputType) -> Boolean): PCollection<InputType> {
    return this.parDo(name) {
        if (function(element)) {
            output(element)
        }
    }
}

/**
 * Map input PCollection
 * nulls are suppressed by default from the output
 * The executed lambda has access to an implicit process context as *this* if needed
 */
inline fun <InputType, OutputType> PCollection<InputType>.map(name: String = "map", crossinline function: (InputType) -> OutputType): PCollection<OutputType> {
    return this.parDo(name) {
        val o = function(element)
        if (o != null) {
            output(function(element))
        }
    }
}

/**
 * FlatMap input PCollection
 * The executed lambda has access to an implicit process context as *this* if needed
 */
inline fun <InputType, OutputType> PCollection<InputType>.flatMap(name: String = "flatMap", crossinline function: (InputType) -> Iterable<OutputType>): PCollection<OutputType> {
    return this.parDo(name) {
        val l = function(element)
        l.forEach {
            output(it)
        }
    }
}

inline fun <reified InputType> PCollection<InputType>.split(name: String = "split", crossinline function: (InputType) -> Boolean): DoFn2Outputs<InputType, InputType> {
    return this.parDo2(name) {
        if (function(element)) {
            output(element)
        } else {
            output2(element)
        }
    }
}