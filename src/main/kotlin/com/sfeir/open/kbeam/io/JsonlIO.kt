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

import com.fasterxml.jackson.databind.ObjectMapper
import com.sfeir.open.kbeam.map
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection

object JsonUtil {
    val mapper: ThreadLocal<ObjectMapper> = ThreadLocal.withInitial {
        com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
    }

    inline fun <reified T> parseString(s: String): T {
        return mapper.get().readValue<T>(s, T::class.java)
    }

    fun <T> dumpString(item: T): String {
        return mapper.get().writeValueAsString(item)
    }
}

inline fun <reified T> Pipeline.readJSONLFile(name: String? = null, noinline config: TextReadConfig.() -> Unit): PCollection<T> {
    return this.readTextFile(name, config).map("Convert JSON to objects") {
        JsonUtil.parseString<T>(it)
    }
}

fun <T> PCollection<T>.writeJSONLFile(name: String? = null, config: TextWriteConfig.() -> Unit) {
    this.map(name = "Serialize to Json") { JsonUtil.dumpString(it) }.writeText(name, config)
}
