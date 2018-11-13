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
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.Watch
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration

/**
 * Reads a single text file as a PCollection of lines
 */
fun Pipeline.readTextFile(name: String? = null, path: String): PCollection<String> {
    return this.apply(name ?: "Read from $path",
            TextIO.read().from(path))
}

fun PCollection<String>.readAllTextFile(name: String? = null): PCollection<String> {
    return this.apply(name ?: "Read from collection",
            TextIO.readAll())
}

fun Pipeline.readTextFileStream(name: String? = null,
                                checkPeriod: Duration = Duration.standardSeconds(30),
                                path: String): PCollection<String> {
    @Suppress("INACCESSIBLE_TYPE")
    return this.apply(name ?: "Streaming Read from $path",
            TextIO.read().from(path).watchForNewFiles(checkPeriod, Watch.Growth.never()))
}