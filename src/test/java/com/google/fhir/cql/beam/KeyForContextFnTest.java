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
package com.google.fhir.cql.beam;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.fhir.cql.beam.types.ResourceTypeAndId;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.elm_modelinfo.r1.ClassInfo;
import org.hl7.elm_modelinfo.r1.ModelInfo;
import org.hl7.elm_modelinfo.r1.RelationshipInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KeyForContextFn}. */
@RunWith(JUnit4.class)
public class KeyForContextFnTest {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  private PCollection<String> testPipelineWith(String... input) {
    return testPipeline.apply(Create.of(ImmutableList.copyOf(input)));
  }

  @Test
  public void keysResourceInContextWithOwnId() {
    String resourceJson = "{\"resourceType\": \"Patient\", \"id\": \"1\"}";

    PCollection<KV<ResourceTypeAndId, String>> output = testPipelineWith(resourceJson)
        .apply(ParDo.of(new KeyForContextFn("Patient", new ModelInfo())));

    PAssert.thatSingleton(output)
        .isEqualTo(KV.of(new ResourceTypeAndId("Patient", "1"), resourceJson));

    testPipeline.run().waitUntilFinish();
  }


  @Test
  public void doesNotKeyResourceOutOfContextWithOwnId() {
    PCollection<KV<ResourceTypeAndId, String>> output =
        testPipelineWith("{\"resourceType\": \"Encounter\", \"id\": \"1\"}")
            .apply(ParDo.of(new KeyForContextFn("Patient", new ModelInfo())));

    PAssert.thatMap(output).isEqualTo(emptyMap());

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void keysOnRelatedField() {
    String resourceJson = "{"
        + "\"resourceType\":\"Encounter\","
        + "\"id\":\"1\","
        + "\"subject\":{\"reference\":\"Patient/2\"}"
        + "}";

    PCollection<KV<ResourceTypeAndId, String>> output = testPipelineWith(resourceJson)
        .apply(ParDo.of(new KeyForContextFn("Patient", new ModelInfo())));

    PAssert.thatSingleton(output)
        .isEqualTo(KV.of(new ResourceTypeAndId("Patient", "2"), resourceJson));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void keysOnMultipleRelatedFields() {
    String resourceJson = "{"
        + "\"resourceType\":\"AllergyIntolerance\","
        + "\"id\":\"1\","
        + "\"patient\":{\"reference\":\"Patient/2\"},"
        + "\"asserter\":{\"reference\":\"Patient/3\"}"
        + "}";

    PCollection<KV<ResourceTypeAndId, String>> output = testPipelineWith(resourceJson)
        .apply(ParDo.of(new KeyForContextFn("Patient",
            new ModelInfo()
                .withTypeInfo(new ClassInfo()
                    .withName("AllergyIntolerance")
                    .withContextRelationship(new RelationshipInfo()
                        .withContext("Patient")
                        .withRelatedKeyElement("asserter"))))));

    PAssert.thatMap(output).isEqualTo(ImmutableMap.of(
        new ResourceTypeAndId("Patient", "2"), resourceJson,
        new ResourceTypeAndId("Patient", "3"), resourceJson));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void failsOnNonRelativeReferences() {
    String resourceJson = "{"
        + "\"resourceType\":\"Encounter\","
        + "\"id\":\"1\","
        + "\"subject\":{\"reference\":\"http://example.com/fhir/Patient/2\"}"
        + "}";

    PCollection<KV<ResourceTypeAndId, String>> output = testPipelineWith(resourceJson)
        .apply(ParDo.of(new KeyForContextFn("Patient", new ModelInfo())));

    PAssert.thatSingleton(output)
        .isEqualTo(KV.of(new ResourceTypeAndId("Patient", "2"), resourceJson));

    Exception thrown = assertThrows(
        PipelineExecutionException.class,
        () -> testPipeline.run().waitUntilFinish());
    assertThat(thrown).hasCauseThat().isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown).hasMessageThat().contains("Unable to handle reference");
  }
}
