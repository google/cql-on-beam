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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.testing.EqualsTester;
import java.math.BigDecimal;
import javax.xml.namespace.QName;
import org.cqframework.cql.elm.execution.ExpressionDef;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GenericExpressionValue}. */
@RunWith(JUnit4.class)
public final class GenericExpressionValueTest {

  @Test
  public void isSupportedExpressionBoolean() {
    ExpressionDef expr =
        new ExpressionDef().withResultTypeName(new QName("urn:hl7-org:elm-types:r1", "Boolean"));
    assertThat(GenericExpressionValue.isSupportedExpression(expr)).isTrue();
  }

  @Test
  public void isSupportedExpressionString() {
    ExpressionDef expr =
        new ExpressionDef().withResultTypeName(new QName("urn:hl7-org:elm-types:r1", "String"));
    assertThat(GenericExpressionValue.isSupportedExpression(expr)).isTrue();
  }

  @Test
  public void isSupportedExpressionInteger() {
    ExpressionDef expr =
        new ExpressionDef().withResultTypeName(new QName("urn:hl7-org:elm-types:r1", "Integer"));
    assertThat(GenericExpressionValue.isSupportedExpression(expr)).isTrue();
  }

  @Test
  public void isSupportedExpressionDecimal() {
    ExpressionDef expr =
        new ExpressionDef().withResultTypeName(new QName("urn:hl7-org:elm-types:r1", "Decimal"));
    assertThat(GenericExpressionValue.isSupportedExpression(expr)).isTrue();
  }

  @Test
  public void fromBoolean() {
    Object value = GenericExpressionValue.from(true);
    assertThat(value).isEqualTo(new GenericExpressionValue(true));
  }

  @Test
  public void fromString() {
    Object value = GenericExpressionValue.from("foo");
    assertThat(value).isEqualTo(new GenericExpressionValue("foo"));
  }

  @Test
  public void fromInteger() {
    Object value = GenericExpressionValue.from(1);
    assertThat(value).isEqualTo(new GenericExpressionValue(1));
  }

  @Test
  public void fromDecimal() {
    Object value = GenericExpressionValue.from(new BigDecimal("1.12"));
    assertThat(value).isEqualTo(new GenericExpressionValue(new BigDecimal("1.12")));
  }

  @Test
  public void getBooleanValue() {
    GenericExpressionValue value = new GenericExpressionValue(true);
    assertThat(value.getBooleanValue()).isEqualTo(true);
    assertThat(value.getValueType()).isEqualTo(GenericExpressionValue.ValueType.BOOLEAN);
  }

  @Test
  public void getStringValue() {
    GenericExpressionValue value = new GenericExpressionValue("foo");
    assertThat(value.getStringValue()).isEqualTo("foo");
    assertThat(value.getValueType()).isEqualTo(GenericExpressionValue.ValueType.STRING);
  }

  @Test
  public void getDecimalValue() {
    GenericExpressionValue value = new GenericExpressionValue(new BigDecimal("0.12"));
    assertThat(value.getDecimalValue()).isEqualTo(0.12);
    assertThat(value.getValueType()).isEqualTo(GenericExpressionValue.ValueType.DECIMAL);
  }

  @Test
  public void getIntValue() {
    GenericExpressionValue value = new GenericExpressionValue(1);
    assertThat(value.getIntValue()).isEqualTo(1);
    assertThat(value.getValueType()).isEqualTo(GenericExpressionValue.ValueType.INTEGER);
  }

  @Test
  public void equals() {
    new EqualsTester()
        .addEqualityGroup(
            new GenericExpressionValue(new BigDecimal("0.123")),
            new GenericExpressionValue(new BigDecimal("0.123")))
        .addEqualityGroup(new GenericExpressionValue(1))
        .addEqualityGroup(new GenericExpressionValue("foo"))
        .addEqualityGroup(new GenericExpressionValue(true))
        .testEquals();
  }
}
