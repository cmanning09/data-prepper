/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public class DoubleConverter implements TypeConverter<Double> {
    public Double convert(Object source) throws IllegalArgumentException {
        if (source instanceof String) {
            return Double.parseDouble((String)source);
        }
        if (source instanceof Double) {
            return (Double)source;
        }
        if (source instanceof Number) {
            return (double)(((Number)source).intValue());
        }
        if (source instanceof Boolean) {
            return (double)(((Boolean)source) ? 1.0 : 0.0);
        }
        throw new IllegalArgumentException("Unsupported type conversion");
    }
}
