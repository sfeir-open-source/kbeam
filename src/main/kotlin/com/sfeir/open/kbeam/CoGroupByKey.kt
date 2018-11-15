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

import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.values.TupleTag

@DefaultCoder(AvroCoder::class)
class CoGroupResults<LeftType, RightType>(private val delegate: CoGbkResult, val leftTag: TupleTag<LeftType>, val rightTag: TupleTag<RightType>) {
    val left: Iterable<LeftType> get() = delegate.getAll(leftTag)
    val right: Iterable<RightType> get() = delegate.getAll(rightTag)

}
/*
inline fun <reified KeyType, reified LeftType, reified RightType> Pipeline.coGroupByKey(
        left: PCollection<KV<KeyType, LeftType>>,
        right: PCollection<KV<KeyType, RightType>>)
        : PCollection<KV<KeyType, CoGroupResults<LeftType, RightType>>> {
    val leftTag = object : TupleTag<LeftType>() {}
    val rightTag = object : TupleTag<RightType>() {}
    val keyedPCollectionTuple = KeyedPCollectionTuple.of(leftTag, left).and(rightTag, right)
    return keyedPCollectionTuple.apply(org.apache.beam.sdk.transforms.join.CoGroupByKey.create()).map { KV.of(it.key, CoGroupResults(it.value, leftTag, rightTag)) }
}
*/