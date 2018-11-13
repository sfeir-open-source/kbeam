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

import org.apache.beam.sdk.Pipeline
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