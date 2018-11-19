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

package com.sfeir.open.kbeam.coder

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptor
import org.objenesis.strategy.StdInstantiatorStrategy
import java.io.*

class KryoCoderProvider(private var kryoBuilder: () -> Kryo = {
    val k = Kryo()
    k.isRegistrationRequired = false
    k.instantiatorStrategy = Kryo.DefaultInstantiatorStrategy(StdInstantiatorStrategy())
    k
})
    : CoderProvider(), Externalizable {

    private var registry: CoderRegistry = CoderRegistry.createDefault()

    override fun readExternal(ois: ObjectInput) {
        @Suppress("UNCHECKED_CAST")
        kryoBuilder = ois.readObject() as () -> Kryo
        kryo = ThreadLocal.withInitial(kryoBuilder)
        registry = CoderRegistry.createDefault()
    }

    override fun writeExternal(out: ObjectOutput) {
        out.writeObject(kryoBuilder)
        out.flush()
    }

    override fun <T : Any> coderFor(typeDescriptor: TypeDescriptor<T>, componentCoders: MutableList<out Coder<*>>): Coder<T> {
        return try {
            registry.getCoder(typeDescriptor)
        } catch (_: CannotProvideCoderException) {
            if (typeDescriptor.rawType == KV::class.java) {
                throw CannotProvideCoderException("Cowardly refusing to encode KV")
            }
            KryoCoder(this, typeDescriptor.rawType)
        }
    }


    private var kryo: ThreadLocal<Kryo> = ThreadLocal.withInitial(kryoBuilder)


    class KryoCoder<T>(private val provider: KryoCoderProvider, private val klazz: Class<in T>) : AtomicCoder<T>(), Serializable {

        override fun encode(value: T, outStream: OutputStream) {
            val out = Output(outStream)
            provider.kryo.get().writeClassAndObject(out, value)
            out.flush()
        }

        override fun decode(inStream: InputStream): T {
            val input = Input(inStream)
            try {
                val o = provider.kryo.get().readClassAndObject(input)
                @Suppress("UNCHECKED_CAST")
                return o as T
            } catch (t: Throwable) {
                throw CoderException(t)
            }
        }

    }
}
