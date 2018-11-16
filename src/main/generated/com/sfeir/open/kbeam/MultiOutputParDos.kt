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

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.*
import org.joda.time.Instant


data class DoFn2Outputs<O1,O2>(val output1: PCollection<O1>,val output2: PCollection<O2>)

class DoFnContextWrapper2Outputs<I, O1,O2>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>) : DoFnContextWrapper<I, O1>(processContext) {

     fun output2(item: O2) {
        outputTagged(tag2, item)
    }

    fun output2Timestamped(item: O2, timestamp: Instant) {
        outputTaggedTimestamped(tag2, item, timestamp)
    }


}

/**
 * Generic parDo extension method for 2 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2> PCollection<InputType>.parDo2(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper2Outputs<InputType, O1,O2>.() -> Unit): DoFn2Outputs<O1,O2> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
    val tagList = TupleTagList.of(listOf(output2Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper2Outputs(context, output2Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn2Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag])
}


data class DoFn3Outputs<O1,O2,O3>(val output1: PCollection<O1>,val output2: PCollection<O2>,val output3: PCollection<O3>)

class DoFnContextWrapper3Outputs<I, O1,O2,O3>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>,private val tag3: TupleTag<O3>) : DoFnContextWrapper<I, O1>(processContext) {

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
 * Generic parDo extension method for 3 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2,reified O3> PCollection<InputType>.parDo3(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper3Outputs<InputType, O1,O2,O3>.() -> Unit): DoFn3Outputs<O1,O2,O3> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
val output3Tag = object : TupleTag<O3>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper3Outputs(context, output2Tag, output3Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn3Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag])
}


data class DoFn4Outputs<O1,O2,O3,O4>(val output1: PCollection<O1>,val output2: PCollection<O2>,val output3: PCollection<O3>,val output4: PCollection<O4>)

class DoFnContextWrapper4Outputs<I, O1,O2,O3,O4>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>,private val tag3: TupleTag<O3>,private val tag4: TupleTag<O4>) : DoFnContextWrapper<I, O1>(processContext) {

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



     fun output4(item: O4) {
        outputTagged(tag4, item)
    }

    fun output4Timestamped(item: O4, timestamp: Instant) {
        outputTaggedTimestamped(tag4, item, timestamp)
    }


}

/**
 * Generic parDo extension method for 4 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2,reified O3,reified O4> PCollection<InputType>.parDo4(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper4Outputs<InputType, O1,O2,O3,O4>.() -> Unit): DoFn4Outputs<O1,O2,O3,O4> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
val output3Tag = object : TupleTag<O3>() {}
val output4Tag = object : TupleTag<O4>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag, output4Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper4Outputs(context, output2Tag, output3Tag, output4Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn4Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag], pCollectionTuple[output4Tag])
}


data class DoFn5Outputs<O1,O2,O3,O4,O5>(val output1: PCollection<O1>,val output2: PCollection<O2>,val output3: PCollection<O3>,val output4: PCollection<O4>,val output5: PCollection<O5>)

class DoFnContextWrapper5Outputs<I, O1,O2,O3,O4,O5>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>,private val tag3: TupleTag<O3>,private val tag4: TupleTag<O4>,private val tag5: TupleTag<O5>) : DoFnContextWrapper<I, O1>(processContext) {

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



     fun output4(item: O4) {
        outputTagged(tag4, item)
    }

    fun output4Timestamped(item: O4, timestamp: Instant) {
        outputTaggedTimestamped(tag4, item, timestamp)
    }



     fun output5(item: O5) {
        outputTagged(tag5, item)
    }

    fun output5Timestamped(item: O5, timestamp: Instant) {
        outputTaggedTimestamped(tag5, item, timestamp)
    }


}

/**
 * Generic parDo extension method for 5 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2,reified O3,reified O4,reified O5> PCollection<InputType>.parDo5(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper5Outputs<InputType, O1,O2,O3,O4,O5>.() -> Unit): DoFn5Outputs<O1,O2,O3,O4,O5> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
val output3Tag = object : TupleTag<O3>() {}
val output4Tag = object : TupleTag<O4>() {}
val output5Tag = object : TupleTag<O5>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag, output4Tag, output5Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper5Outputs(context, output2Tag, output3Tag, output4Tag, output5Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn5Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag], pCollectionTuple[output4Tag], pCollectionTuple[output5Tag])
}


data class DoFn6Outputs<O1,O2,O3,O4,O5,O6>(val output1: PCollection<O1>,val output2: PCollection<O2>,val output3: PCollection<O3>,val output4: PCollection<O4>,val output5: PCollection<O5>,val output6: PCollection<O6>)

class DoFnContextWrapper6Outputs<I, O1,O2,O3,O4,O5,O6>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>,private val tag3: TupleTag<O3>,private val tag4: TupleTag<O4>,private val tag5: TupleTag<O5>,private val tag6: TupleTag<O6>) : DoFnContextWrapper<I, O1>(processContext) {

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



     fun output4(item: O4) {
        outputTagged(tag4, item)
    }

    fun output4Timestamped(item: O4, timestamp: Instant) {
        outputTaggedTimestamped(tag4, item, timestamp)
    }



     fun output5(item: O5) {
        outputTagged(tag5, item)
    }

    fun output5Timestamped(item: O5, timestamp: Instant) {
        outputTaggedTimestamped(tag5, item, timestamp)
    }



     fun output6(item: O6) {
        outputTagged(tag6, item)
    }

    fun output6Timestamped(item: O6, timestamp: Instant) {
        outputTaggedTimestamped(tag6, item, timestamp)
    }


}

/**
 * Generic parDo extension method for 6 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2,reified O3,reified O4,reified O5,reified O6> PCollection<InputType>.parDo6(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper6Outputs<InputType, O1,O2,O3,O4,O5,O6>.() -> Unit): DoFn6Outputs<O1,O2,O3,O4,O5,O6> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
val output3Tag = object : TupleTag<O3>() {}
val output4Tag = object : TupleTag<O4>() {}
val output5Tag = object : TupleTag<O5>() {}
val output6Tag = object : TupleTag<O6>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag, output4Tag, output5Tag, output6Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper6Outputs(context, output2Tag, output3Tag, output4Tag, output5Tag, output6Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn6Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag], pCollectionTuple[output4Tag], pCollectionTuple[output5Tag], pCollectionTuple[output6Tag])
}


data class DoFn7Outputs<O1,O2,O3,O4,O5,O6,O7>(val output1: PCollection<O1>,val output2: PCollection<O2>,val output3: PCollection<O3>,val output4: PCollection<O4>,val output5: PCollection<O5>,val output6: PCollection<O6>,val output7: PCollection<O7>)

class DoFnContextWrapper7Outputs<I, O1,O2,O3,O4,O5,O6,O7>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>,private val tag3: TupleTag<O3>,private val tag4: TupleTag<O4>,private val tag5: TupleTag<O5>,private val tag6: TupleTag<O6>,private val tag7: TupleTag<O7>) : DoFnContextWrapper<I, O1>(processContext) {

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



     fun output4(item: O4) {
        outputTagged(tag4, item)
    }

    fun output4Timestamped(item: O4, timestamp: Instant) {
        outputTaggedTimestamped(tag4, item, timestamp)
    }



     fun output5(item: O5) {
        outputTagged(tag5, item)
    }

    fun output5Timestamped(item: O5, timestamp: Instant) {
        outputTaggedTimestamped(tag5, item, timestamp)
    }



     fun output6(item: O6) {
        outputTagged(tag6, item)
    }

    fun output6Timestamped(item: O6, timestamp: Instant) {
        outputTaggedTimestamped(tag6, item, timestamp)
    }



     fun output7(item: O7) {
        outputTagged(tag7, item)
    }

    fun output7Timestamped(item: O7, timestamp: Instant) {
        outputTaggedTimestamped(tag7, item, timestamp)
    }


}

/**
 * Generic parDo extension method for 7 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2,reified O3,reified O4,reified O5,reified O6,reified O7> PCollection<InputType>.parDo7(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper7Outputs<InputType, O1,O2,O3,O4,O5,O6,O7>.() -> Unit): DoFn7Outputs<O1,O2,O3,O4,O5,O6,O7> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
val output3Tag = object : TupleTag<O3>() {}
val output4Tag = object : TupleTag<O4>() {}
val output5Tag = object : TupleTag<O5>() {}
val output6Tag = object : TupleTag<O6>() {}
val output7Tag = object : TupleTag<O7>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag, output4Tag, output5Tag, output6Tag, output7Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper7Outputs(context, output2Tag, output3Tag, output4Tag, output5Tag, output6Tag, output7Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn7Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag], pCollectionTuple[output4Tag], pCollectionTuple[output5Tag], pCollectionTuple[output6Tag], pCollectionTuple[output7Tag])
}


data class DoFn8Outputs<O1,O2,O3,O4,O5,O6,O7,O8>(val output1: PCollection<O1>,val output2: PCollection<O2>,val output3: PCollection<O3>,val output4: PCollection<O4>,val output5: PCollection<O5>,val output6: PCollection<O6>,val output7: PCollection<O7>,val output8: PCollection<O8>)

class DoFnContextWrapper8Outputs<I, O1,O2,O3,O4,O5,O6,O7,O8>(processContext: DoFn<I, O1>.ProcessContext, private val tag2: TupleTag<O2>,private val tag3: TupleTag<O3>,private val tag4: TupleTag<O4>,private val tag5: TupleTag<O5>,private val tag6: TupleTag<O6>,private val tag7: TupleTag<O7>,private val tag8: TupleTag<O8>) : DoFnContextWrapper<I, O1>(processContext) {

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



     fun output4(item: O4) {
        outputTagged(tag4, item)
    }

    fun output4Timestamped(item: O4, timestamp: Instant) {
        outputTaggedTimestamped(tag4, item, timestamp)
    }



     fun output5(item: O5) {
        outputTagged(tag5, item)
    }

    fun output5Timestamped(item: O5, timestamp: Instant) {
        outputTaggedTimestamped(tag5, item, timestamp)
    }



     fun output6(item: O6) {
        outputTagged(tag6, item)
    }

    fun output6Timestamped(item: O6, timestamp: Instant) {
        outputTaggedTimestamped(tag6, item, timestamp)
    }



     fun output7(item: O7) {
        outputTagged(tag7, item)
    }

    fun output7Timestamped(item: O7, timestamp: Instant) {
        outputTaggedTimestamped(tag7, item, timestamp)
    }



     fun output8(item: O8) {
        outputTagged(tag8, item)
    }

    fun output8Timestamped(item: O8, timestamp: Instant) {
        outputTaggedTimestamped(tag8, item, timestamp)
    }


}

/**
 * Generic parDo extension method for 8 outputs
 *
 * The executed lambda has access to an implicit process context as *this* with typed outputs: *output* and *output2*
 * @param name The name of the processing step
 * @param sideInputs The *optional* sideInputs
 */
inline fun <InputType, reified O1,reified O2,reified O3,reified O4,reified O5,reified O6,reified O7,reified O8> PCollection<InputType>.parDo8(name: String = "ParDo",
                                                                                              sideInputs: List<PCollectionView<*>> = emptyList()
                                                                                              , crossinline function: DoFnContextWrapper8Outputs<InputType, O1,O2,O3,O4,O5,O6,O7,O8>.() -> Unit): DoFn8Outputs<O1,O2,O3,O4,O5,O6,O7,O8> {
val output1Tag = object : TupleTag<O1>() {}
val output2Tag = object : TupleTag<O2>() {}
val output3Tag = object : TupleTag<O3>() {}
val output4Tag = object : TupleTag<O4>() {}
val output5Tag = object : TupleTag<O5>() {}
val output6Tag = object : TupleTag<O6>() {}
val output7Tag = object : TupleTag<O7>() {}
val output8Tag = object : TupleTag<O8>() {}
    val tagList = TupleTagList.of(listOf(output2Tag, output3Tag, output4Tag, output5Tag, output6Tag, output7Tag, output8Tag))
    val pCollectionTuple = this.apply(name, ParDo.of(object : DoFn<InputType, O1>() {
        @ProcessElement
        fun processElement(context: ProcessContext) {
            DoFnContextWrapper8Outputs(context, output2Tag, output3Tag, output4Tag, output5Tag, output6Tag, output7Tag, output8Tag).apply(function)
        }
    }).withSideInputs(sideInputs).withOutputTags(output1Tag, tagList))
    return DoFn8Outputs(pCollectionTuple[output1Tag], pCollectionTuple[output2Tag], pCollectionTuple[output3Tag], pCollectionTuple[output4Tag], pCollectionTuple[output5Tag], pCollectionTuple[output6Tag], pCollectionTuple[output7Tag], pCollectionTuple[output8Tag])
}
