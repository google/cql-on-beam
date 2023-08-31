/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.fhir.cql.beam.types;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.cqframework.cql.elm.execution.ExpressionDef;

/**
 * A generic container that can hold different CQL expression result types and can be serialized by
 * {@link AvroCoder}.
 */
@DefaultCoder(AvroCoder.class)
public final class GenericExpressionValue implements Serializable {

  enum ValueType {
    BOOLEAN,
    STRING,
    INTEGER,
    DECIMAL
  }

  @Nullable private Boolean booleanValue;

  @Nullable private String stringValue;

  @Nullable private Double decimalValue;

  @Nullable private Integer intValue;

  private ValueType valueType;

  // Required for AvroCoder.
  public GenericExpressionValue() {}

  public GenericExpressionValue(@Nullable Boolean value) {
    this.booleanValue = value;
    this.valueType = ValueType.BOOLEAN;
  }

  public GenericExpressionValue(@Nullable String value) {
    this.stringValue = value;
    this.valueType = ValueType.STRING;
  }

  public GenericExpressionValue(@Nullable BigDecimal value) {
    // We convert the BigDecimal to Double for SQL arithmetics to work out of the box.
    // This is okay because based on the documentation, we will only need to represent 8 decimal
    // places accurately: https://cql.hl7.org/09-b-cqlreference.html#decimal-1
    this.decimalValue = value.doubleValue();
    this.valueType = ValueType.DECIMAL;
  }

  public GenericExpressionValue(@Nullable Integer value) {
    this.intValue = value;
    this.valueType = ValueType.INTEGER;
  }

  @Nullable
  public Boolean getBooleanValue() {
    if (valueType == ValueType.BOOLEAN) {
      return booleanValue;
    }
    throw new IllegalStateException("booleanValue is not set.");
  }

  @Nullable
  public String getStringValue() {

    if (valueType == ValueType.STRING) {
      return stringValue;
    }
    throw new IllegalStateException("stringValue is not set.");
  }

  @Nullable
  public Double getDecimalValue() {

    if (valueType == ValueType.DECIMAL) {
      return decimalValue;
    }
    throw new IllegalStateException("decimalValue is not set.");
  }

  @Nullable
  public Integer getIntValue() {

    if (valueType == ValueType.INTEGER) {
      return intValue;
    }
      throw new IllegalStateException("intValue is not set.");
  }
  
  public ValueType getValueType() {
    return valueType;
  }

  /**
   * Creates and returns a GenericExpressionValue based on the type of the given the value. It
   * throws IllegalArgumentException if isSupportedExpression returns false on the provided value.
   */
  public static GenericExpressionValue from(@Nullable Object value) {
    if (value instanceof String) {
      return new GenericExpressionValue((String) value);
    } else if (value instanceof Boolean) {
      return new GenericExpressionValue((Boolean) value);
    } else if (value instanceof BigDecimal) {
      return new GenericExpressionValue((BigDecimal) value);
    } else if (value instanceof Integer) {
      return new GenericExpressionValue((Integer) value);
    } else {
      throw new IllegalArgumentException(
          "Only objects of type String, Boolean, BigDecimal, Integer are supported. Got "
              + value.getClass().getName());
    }
  }

  /** Checks that the type of the given CQL expression is supported. */
  public static boolean isSupportedExpression(@Nullable ExpressionDef expression) {
    return expression.getResultTypeName() != null
        && expression.getResultTypeName().getNamespaceURI().equals("urn:hl7-org:elm-types:r1")
        && (expression.getResultTypeName().getLocalPart().equals("Boolean")
            || expression.getResultTypeName().getLocalPart().equals("String")
            || expression.getResultTypeName().getLocalPart().equals("Decimal")
            || expression.getResultTypeName().getLocalPart().equals("Integer"));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(GenericExpressionValue.class)
        .add("booleanValue", booleanValue)
        .add("stringValue", stringValue)
        .add("decimalValue", decimalValue)
        .add("intValue", intValue)
        .add("valueType", valueType)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.booleanValue, this.stringValue, this.decimalValue, this.intValue, this.valueType);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof GenericExpressionValue)) {
      return false;
    }

    GenericExpressionValue that = (GenericExpressionValue) other;

    return Objects.equals(this.booleanValue, that.booleanValue)
        && Objects.equals(this.stringValue, that.stringValue)
        && Objects.equals(this.decimalValue, that.decimalValue)
        && Objects.equals(this.intValue, that.intValue)
        && Objects.equals(this.valueType, that.valueType);
  }
}
