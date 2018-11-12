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