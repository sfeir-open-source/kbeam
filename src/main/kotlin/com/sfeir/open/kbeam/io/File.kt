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

package com.sfeir.open.kbeam.io

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.Watch
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration

open class WriteConfig(
        var path: String? = null,
        var compression: Compression = Compression.UNCOMPRESSED,
        var suffix: String = ".txt",
        var numShards: Int = 0) {
    operator fun <DT, UT> invoke(io: FileIO.Write<DT, UT>): FileIO.Write<DT, UT>? {
        return io
                .to(path)
                .withCompression(compression)
                .withNumShards(numShards)
                .withSuffix(suffix)
    }
}

open class TextWriteConfig(var delimiter: String = "\r\n",
                           var header: String = "",
                           var footer: String = ""
) : WriteConfig() {
    operator fun invoke(io: TextIO.Write): TextIO.Write {
        return io
                .to(path)
                .withCompression(compression)
                .withNumShards(numShards)
                .withSuffix(suffix)
                .withDelimiter(delimiter.toCharArray())
                .withHeader(header)
                .withFooter(footer)
    }
}

class TextReadConfig(var filePattern: String? = null,
                     var compression: Compression = Compression.UNCOMPRESSED,
                     var delimiter: ByteArray = "\r\n".toByteArray(Charsets.US_ASCII),
                     var watchForNewFiles: Boolean = false,
                     var checkPeriod: Duration = Duration.standardMinutes(1)
) {
    operator fun invoke(io: TextIO.Read): TextIO.Read {
        if (watchForNewFiles) {
            // Streaming case
            @Suppress("INACCESSIBLE_TYPE")
            return io
                    .from(filePattern)
                    .withCompression(compression)
                    .withDelimiter(delimiter)
                    .watchForNewFiles(checkPeriod, Watch.Growth.never())
        } else {
            return io
                    .from(filePattern)
                    .withCompression(compression)
                    .withDelimiter(delimiter)
        }

    }
}

/**
 * Reads a text files as a PCollection of lines
 */
fun Pipeline.readTextFile(name: String? = null, configurator: TextReadConfig.() -> Unit): PCollection<String> {
    val readConfig: TextReadConfig = TextReadConfig()
    configurator(readConfig)
    if (readConfig.watchForNewFiles) {
        // Streaming case
        return this.apply(name ?: "Read from ${readConfig.filePattern}",
                readConfig(TextIO.read()))
    } else {
        // Batch case
        return this.apply(name ?: "Read from ${readConfig.filePattern}",
                readConfig(TextIO.read()))
    }

}

/**
 * Read files from a PCollection of file names
 */
fun PCollection<String>.readAllTextFiles(name: String? = null): PCollection<String> {
    return this.apply(name ?: "Read from collection", TextIO.readAll()
            .withCompression(Compression.AUTO))
}

fun PCollection<String>.writeText(name: String? = null, configFunction: (TextWriteConfig.() -> Unit)) {
    val config = TextWriteConfig()
    configFunction(config)
    if (config.path == null) {
        throw IllegalArgumentException("Must define a file path")
    }
    val t = config(TextIO.write())
    this.apply(name ?: "Writing to ${config.path}", t)
}