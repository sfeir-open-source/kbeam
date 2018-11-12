package com.sfeir.open.kbeam

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.junit.jupiter.api.Test

class TestCoGroupByKey {
    @Test
    fun runTest() {
        val (pipeline, options) = PipeBuilder.create<PipelineOptions>(arrayOf())

        val list1 = (1..1000).map { KV.of("Key_${it % 10})", it) }
        val list2 = (0..9).flatMap {
            val k = it
            ('a'..'z').map { KV.of("Key_$k)", "$it") }
        }
        val plist1 = pipeline.apply("Create List1", Create.of(list1))
        val plist2 = pipeline.apply("Create List2", Create.of(list2))
        val group = pipeline.coGroupByKey(plist1, plist2)
        group.map("Print groupings") {
            val k = it.key
            val l1 = it.value.left
            val l2 = it.value.right
            println("$k : $l1 $l2")
            k
        }
    }
}