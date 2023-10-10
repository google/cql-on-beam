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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.fhir.cql.beam.types.CqlEvaluationResult;
import com.google.fhir.cql.beam.types.CqlLibraryId;
import com.google.fhir.cql.beam.types.GenericExpressionValue;
import com.google.fhir.cql.beam.types.MeasurementPeriod;
import com.google.fhir.cql.beam.types.ResourceTypeAndId;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.cqframework.cql.cql2elm.CqlCompiler;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions.Options;
import org.cqframework.cql.cql2elm.LibraryManager;
import org.cqframework.cql.cql2elm.ModelManager;
import org.cqframework.cql.elm.execution.Library;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.opencds.cqf.cql.evaluator.engine.elm.LibraryMapper;

/** Tests for {@link EvaluateCqlForContextFn}. */
@RunWith(JUnit4.class)
public class EvaluateCqlForContextFnTest {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  private static final ResourceTypeAndId PATIENT_1_ID = new ResourceTypeAndId("Patient", "1");
  private static final ResourceTypeAndId PATIENT_2_ID = new ResourceTypeAndId("Patient", "2");
  private static final String PATIENT_1_AGE_21_RESOURCE_JSON =
      "{\"resourceType\": \"Patient\", \"id\": \"1\", \"birthDate\": \"2000-01-01\"}";
  private static final String PATIENT_2_AGE_1_RESOURCE_JSON =
      "{\"resourceType\": \"Patient\", \"id\": \"2\", \"birthDate\": \"2020-01-01\"}";
  private static final ZonedDateTime EVALUATION_TIME =
      ZonedDateTime.of(2022, 1, 7, 13, 14, 15, 0, ZoneOffset.UTC);

  private static CqlLibraryId cqlLibraryId(String name, String version) {
    return new CqlLibraryId(name, version);
  }

  private static VersionedIdentifier versionedIdentifier(String id, String version) {
    VersionedIdentifier versionedIdentifier = new VersionedIdentifier();
    versionedIdentifier.setId(id);
    versionedIdentifier.setVersion(version);
    return versionedIdentifier;
  }

  private static Collection<Library> cqlToLibraries(String... cqlStrings) {
    ModelManager modelManager = new ModelManager();
    CqlCompiler compiler = new CqlCompiler(modelManager, new LibraryManager(modelManager));

    Collection<Library> libraries = new ArrayList<>();
    try {
      for (String cql : cqlStrings) {
        libraries.add(LibraryMapper.INSTANCE.map(compiler.run(
            cql, Options.EnableResultTypes, Options.EnableLocators)));
        assertThat(compiler.getExceptions()).isEmpty();
      }
      return libraries;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void libraryIdPopulated() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "define \"Exp1\": true"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getLibraryId()).isEqualTo(new CqlLibraryId("FooLibrary", "0.1"));
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void libraryVersionPopulatedWhenNotSpecified() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "define \"Exp1\": true"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", null)),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getLibraryId().getVersion()).isEqualTo("0.1");
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void contextPopulated() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "define \"Exp1\": true"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getContexId()).isEqualTo(PATIENT_1_ID);
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void resultsPopulatedBoolean() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "define \"Exp1\": true"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output)
        .satisfies(
            result -> {
              assertThat(result.getResults())
                  .containsExactly("Exp1", new GenericExpressionValue(true));
              return null;
            });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void resultsPopulatedDecimal() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input =
        ImmutableMap.of(PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output =
        testPipeline
            .apply(Create.of(input))
            .apply(
                ParDo.of(
                    new EvaluateCqlForContextFn(
                        cqlToLibraries(
                            "library FooLibrary version '0.1'\n"
                                + "using FHIR version '4.0.1'\n"
                                + "define \"Exp1\": 5.0"),
                        ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
                        ImmutableList.of(),
                        EVALUATION_TIME,
                        FhirVersionEnum.R4)));

    PAssert.thatSingleton(output)
        .satisfies(
            result -> {
              assertThat(result.getResults())
                  .containsExactly("Exp1", new GenericExpressionValue(new BigDecimal("5.0")));
              return null;
            });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void resultsPopulatedString() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input =
        ImmutableMap.of(PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output =
        testPipeline
            .apply(Create.of(input))
            .apply(
                ParDo.of(
                    new EvaluateCqlForContextFn(
                        cqlToLibraries(
                            "library FooLibrary version '0.1'\n"
                                + "using FHIR version '4.0.1'\n"
                                + "define \"Exp1\": 'foo'"),
                        ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
                        ImmutableList.of(),
                        EVALUATION_TIME,
                        FhirVersionEnum.R4)));

    PAssert.thatSingleton(output)
        .satisfies(
            result -> {
              assertThat(result.getResults())
                  .containsExactly("Exp1", new GenericExpressionValue("foo"));
              return null;
            });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void resultsPopulatedInteger() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input =
        ImmutableMap.of(PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output =
        testPipeline
            .apply(Create.of(input))
            .apply(
                ParDo.of(
                    new EvaluateCqlForContextFn(
                        cqlToLibraries(
                            "library FooLibrary version '0.1'\n"
                                + "using FHIR version '4.0.1'\n"
                                + "define \"Exp1\": 1"),
                        ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
                        ImmutableList.of(),
                        EVALUATION_TIME,
                        FhirVersionEnum.R4)));

    PAssert.thatSingleton(output)
        .satisfies(
            result -> {
              assertThat(result.getResults())
                  .containsExactly("Exp1", new GenericExpressionValue(1));
              return null;
            });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void nullResultsArePopulated() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of("{\"resourceType\": \"Patient\", \"id\": \"1\"}"));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Patient\n"
                    + "define \"Exp1\": AgeInYears() < 15"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output)
        .satisfies(
            result -> {
              assertThat(result.getResults()).containsExactly("Exp1", null);
              return null;
            });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void valueSetsAreLoaded() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "valueset \"FooSet\": 'http://example.com/foovalueset'\n"
                    + "codesystem \"FooSystem\": 'http://example.com/foosystem'\n"
                    + "code \"Code3\": '3' from \"FooSystem\"\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Patient\n"
                    + "define \"Exp1\": \"Code3\" in \"FooSet\""),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of("{"
                + "\"resourceType\": \"ValueSet\","
                + "\"url\": \"http://example.com/foovalueset\","
                + "\"expansion\": {"
                + "  \"contains\": ["
                + "    {"
                + "      \"system\": \"http://example.com/foosystem\","
                + "      \"code\": \"3\""
                + "    }"
                + "  ]"
                + "  }"
                + "}"),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output)
        .satisfies(
            result -> {
              assertThat(result.getResults())
                  .containsExactly("Exp1", new GenericExpressionValue(true));
              return null;
            });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void missingValueSet() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "valueset \"FooSet\": 'http://example.com/foovalueset'\n"
                    + "codesystem \"FooSystem\": 'http://example.com/foosystem'\n"
                    + "code \"Code3\": '3' from \"FooSystem\"\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Patient\n"
                    + "define \"Exp1\": \"Code3\" in \"FooSet\""),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getError()).contains("http://example.com/foovalueset");
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void expressionsThatDontMatchContextAreSkipped() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Unfiltered\n"
                    + "define \"Exp1\": true"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getResults()).isEmpty();
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void expressionsThatAreNotSupportedAreSkipped() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output =
        testPipeline
            .apply(Create.of(input))
            .apply(
                ParDo.of(
                    new EvaluateCqlForContextFn(
                        cqlToLibraries(
                            "library FooLibrary version '0.1'\n"
                                + "using FHIR version '4.0.1'\n"
                                + "context Patient\n"
                                + "define \"Exp1\": @2013-01-01"),
                        ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
                        ImmutableList.of(),
                        EVALUATION_TIME,
                        FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getResults()).isEmpty();
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void thrownExceptionsCapturedInResults() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Patient\n"
                    + "define \"Exp1\": (Interval[3, 1].high > 5)"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getResults()).isNull();
      assertThat(result.getError()).contains("Invalid Interval");
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void contextSwitchingForbidden() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Unfiltered\n"
                    + "define \"Exp1\": true\n"
                    + "context Patient\n"
                    + "define \"Exp2\": \"Exp1\""),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.thatSingleton(output).satisfies(result -> {
      assertThat(result.getError()).contains("Context switching is not supported.");
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void multipleLibraries() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "define \"Exp1\": true",
                "library BarLibrary version '0.5'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "define \"ExpA\": false"),
            ImmutableSet.of(
                cqlLibraryId("FooLibrary", "0.1"),
                cqlLibraryId("BarLibrary", "0.5")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.that(output)
        .containsInAnyOrder(
            new CqlEvaluationResult(
                versionedIdentifier("FooLibrary", "0.1"),
                PATIENT_1_ID,
                EVALUATION_TIME,
                new MeasurementPeriod(),
                ImmutableMap.of("Exp1", new GenericExpressionValue(true))),
            new CqlEvaluationResult(
                versionedIdentifier("BarLibrary", "0.5"),
                PATIENT_1_ID,
                EVALUATION_TIME,
                new MeasurementPeriod(),
                ImmutableMap.of("ExpA", new GenericExpressionValue(false))));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void multiplePatients() {
    ImmutableMap<ResourceTypeAndId, Iterable<String>> input = ImmutableMap.of(
        PATIENT_1_ID, ImmutableList.of(PATIENT_1_AGE_21_RESOURCE_JSON),
        PATIENT_2_ID, ImmutableList.of(PATIENT_2_AGE_1_RESOURCE_JSON));

    PCollection<CqlEvaluationResult> output = testPipeline.apply(Create.of(input))
        .apply(ParDo.of(new EvaluateCqlForContextFn(
            cqlToLibraries(
                "library FooLibrary version '0.1'\n"
                    + "using FHIR version '4.0.1'\n"
                    + "context Patient\n"
                    + "define \"YoungerThan18\": AgeInYears() < 18"),
            ImmutableSet.of(cqlLibraryId("FooLibrary", "0.1")),
            ImmutableList.of(),
            EVALUATION_TIME,
            FhirVersionEnum.R4)));

    PAssert.that(output)
        .containsInAnyOrder(
            new CqlEvaluationResult(
                versionedIdentifier("FooLibrary", "0.1"),
                PATIENT_1_ID,
                EVALUATION_TIME,
                new MeasurementPeriod(),
                ImmutableMap.of("YoungerThan18", new GenericExpressionValue(false))),
            new CqlEvaluationResult(
                versionedIdentifier("FooLibrary", "0.1"),
                PATIENT_2_ID,
                EVALUATION_TIME,
                new MeasurementPeriod(),
                ImmutableMap.of("YoungerThan18", new GenericExpressionValue(true))));

    testPipeline.run().waitUntilFinish();
  }
}
