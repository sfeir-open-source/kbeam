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