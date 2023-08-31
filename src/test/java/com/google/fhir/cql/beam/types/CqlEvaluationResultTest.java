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
import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CqlEvaluationResult}. */
@RunWith(JUnit4.class)
public class CqlEvaluationResultTest {
  private static final ZonedDateTime EVALUATION_TIME_1 =
      ZonedDateTime.of(2022, 1, 1, 1, 1, 1, 1, ZoneOffset.UTC);
  private static final ZonedDateTime EVALUATION_TIME_2 =
      ZonedDateTime.of(2022, 2, 2, 2, 2, 2, 2, ZoneOffset.UTC);

  private static final ResourceTypeAndId PATIENT_1 = new ResourceTypeAndId("Patient", "1");

  private static final VersionedIdentifier libraryBar1 =
      new VersionedIdentifier().withId("Bar").withVersion("1");
  private static final VersionedIdentifier libraryBar2 =
      new VersionedIdentifier().withId("Bar").withVersion("2");

  @Test
  public void getLibraryId() {
    CqlEvaluationResult result =
        new CqlEvaluationResult(libraryBar1, PATIENT_1, EVALUATION_TIME_1, emptyMap());

    assertThat(result.getLibraryId()).isEqualTo(new CqlLibraryId(libraryBar1));
  }

  @Test
  public void getContextId() {
    CqlEvaluationResult result =
        new CqlEvaluationResult(libraryBar1, PATIENT_1, EVALUATION_TIME_1, emptyMap());

    assertThat(result.getContexId()).isEqualTo(PATIENT_1);
  }

  @Test
  public void getEvaluationTime_truncatesToMilliseconds() {
    CqlEvaluationResult result =
        new CqlEvaluationResult(libraryBar1, PATIENT_1, EVALUATION_TIME_1, emptyMap());

    assertThat(result.getEvaluationTime())
        .isEqualTo(EVALUATION_TIME_1.truncatedTo(ChronoUnit.MILLIS).toInstant());
  }

  @Test
  public void getError() {
    CqlEvaluationResult result = new CqlEvaluationResult(
        libraryBar1, PATIENT_1, EVALUATION_TIME_1, new RuntimeException("An exception"));

    assertThat(result.getError()).isEqualTo("An exception");
  }

  @Test
  public void getError_returnsNullWhenNoErrorExists() {
    CqlEvaluationResult result =
        new CqlEvaluationResult(libraryBar1, PATIENT_1, EVALUATION_TIME_1, emptyMap());

    assertThat(result.getError()).isNull();
  }


  @Test
  public void getResults_returnsNullWhenErrorExists() {
    CqlEvaluationResult result = new CqlEvaluationResult(
        libraryBar1, PATIENT_1, EVALUATION_TIME_1, new RuntimeException("An exception"));

    assertThat(result.getResults()).isNull();
  }

  @Test
  public void getResults() {
    Map<String, GenericExpressionValue> results =
        ImmutableMap.of(
            "Foo", new GenericExpressionValue(true), "Bar", new GenericExpressionValue(false));

    CqlEvaluationResult result =
        new CqlEvaluationResult(libraryBar1, PATIENT_1, EVALUATION_TIME_1, results);

    assertThat(result.getResults()).isEqualTo(results);
  }

  @Test
  public void equals() {
    new EqualsTester()
        .addEqualityGroup(
            new CqlEvaluationResult(
                libraryBar1,
                PATIENT_1,
                EVALUATION_TIME_1,
                ImmutableMap.of("Numerator", new GenericExpressionValue(false))),
            new CqlEvaluationResult(
                libraryBar1,
                PATIENT_1,
                EVALUATION_TIME_1,
                ImmutableMap.of("Numerator", new GenericExpressionValue(false))))
        .addEqualityGroup(
            new CqlEvaluationResult(libraryBar1, PATIENT_1, EVALUATION_TIME_1, ImmutableMap.of()))
        .addEqualityGroup(
            new CqlEvaluationResult(libraryBar2, PATIENT_1, EVALUATION_TIME_1, ImmutableMap.of()))
        .addEqualityGroup(
            new CqlEvaluationResult(libraryBar2, PATIENT_1, EVALUATION_TIME_2, ImmutableMap.of()))
        .addEqualityGroup(
            new CqlEvaluationResult(
                libraryBar1, PATIENT_1, EVALUATION_TIME_1, new RuntimeException("Exception")),
            new CqlEvaluationResult(
                libraryBar1, PATIENT_1, EVALUATION_TIME_1, new RuntimeException("Exception")))
        .addEqualityGroup(
            new CqlEvaluationResult(
                libraryBar1, PATIENT_1, EVALUATION_TIME_1, new RuntimeException("Other exception")))
        .addEqualityGroup(
            new CqlEvaluationResult(
                libraryBar1, PATIENT_1, EVALUATION_TIME_2, new RuntimeException("Exception")))
        .testEquals();
  }
}
