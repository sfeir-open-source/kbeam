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

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TupleTag


@Suppress("unused")
inline fun <reified KeyType, reified LeftType, reified RightType, reified OutputType> Pipeline.coGroupByKey(
        left: PCollection<KV<KeyType, LeftType>>,
        right: PCollection<KV<KeyType, RightType>>, crossinline function: (key: KeyType, left: Iterable<LeftType>, right: Iterable<RightType>) -> List<OutputType>)
        : PCollection<OutputType> {
    val leftTag = object : TupleTag<LeftType>() {}
    val rightTag = object : TupleTag<RightType>() {}
    val keyedPCollectionTuple = KeyedPCollectionTuple.of(leftTag, left).and(rightTag, right)
    return keyedPCollectionTuple.apply(CoGroupByKey.create()).parDo {
        val k = element.key
        if (k != null) {
            function(k, element.value.getAll(leftTag), element.value.getAll(rightTag)).forEach { output(it) }
        }
    }
}
