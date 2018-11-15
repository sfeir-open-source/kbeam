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

import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView

/**
 * Converts the input PCollection into a list based PCollectionView
 *
 * **Warning** The output will be memory bound, use [toIterable] for large sets
 */
fun <InputType> PCollection<InputType>.toList(): PCollectionView<List<InputType>> {
    return this.apply(View.asList())
}

/**
 * Converts the input PCollection into an Iterable based PCollectionView
 *
 * Runtime environment *should* optimize for disk based stores and avoid storing the broadcast collection on the Heap
 */
fun <InputType> PCollection<InputType>.toIterable(): PCollectionView<Iterable<InputType>> {
    return this.apply(View.asIterable())
}

fun <K, V> PCollection<KV<K, V>>.toMap(): PCollectionView<Map<K, V>> {
    return this.apply(View.asMap())
}