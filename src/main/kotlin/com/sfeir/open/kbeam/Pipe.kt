package com.sfeir.open.kbeam

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory

/**
 * Utility methods for Pipelines
 */
object PipeBuilder {
    /**
     * Create a configured pipeline from arguments
     * Custom options class should be passed as a type parameter
     * @return (the pipeline, typed options)
     */
    inline fun <reified R : PipelineOptions> create(args: Array<String>): Pair<Pipeline, R> {
        val options = PipelineOptionsFactory.fromArgs(*args)
                .withValidation()
                .`as`(R::class.java)
        val res = Pair(Pipeline.create(options), options)
        return res
    }

}