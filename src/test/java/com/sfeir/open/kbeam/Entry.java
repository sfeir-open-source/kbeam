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

package com.sfeir.open.kbeam;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Entry {
    private String name;
    private String countryCode;
    private Double doubleValue;
    private String countryName;

    private Entry() {
    }

    Entry(String name, String countryCode, Double doubleValue) {
        this(name, countryCode, doubleValue, "Unknown");
    }

    Entry(String name, String countryCode, Double doubleValue, String countryName) {
        this.name = name;
        this.countryCode = countryCode;
        this.doubleValue = doubleValue;
        this.countryName = countryName;
    }

    String getName() {
        return name;
    }

    String getCountryCode() {
        return countryCode;
    }

    Double getDoubleValue() {
        return doubleValue;
    }

    public String getCountryName() {
        return countryName;
    }

    @Override
    public String toString() {
        return "KEntry{" +
                "name='" + name + '\'' +
                ", countryCode='" + countryCode + '\'' +
                ", doubleValue=" + doubleValue +
                ", countryName='" + countryName + '\'' +
                '}';
    }
}