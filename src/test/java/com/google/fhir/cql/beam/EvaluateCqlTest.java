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
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.fhir.cql.beam.EvaluateCql.EvaluateCqlOptions;
import com.google.fhir.cql.beam.types.CqlEvaluationResult;
import com.google.fhir.cql.beam.types.GenericExpressionValue;
import com.google.fhir.cql.beam.types.ResourceTypeAndId;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.cqframework.cql.cql2elm.CqlTranslatorIncludeException;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link EvaluateCql}. */
@RunWith(JUnit4.class)
public class EvaluateCqlTest {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  private static final ResourceTypeAndId PATIENT_1_ID = new ResourceTypeAndId("Patient", "1");
  private static final ResourceTypeAndId PATIENT_2_ID = new ResourceTypeAndId("Patient", "2");
  private static final String PATIENT_1_RESOURCE_JSON =
      "{\"resourceType\": \"Patient\", \"id\": \"1\"}";
  private static final String PATIENT_2_RESOURCE_JSON =
      "{\"resourceType\": \"Patient\", \"id\": \"2\"}";
  private static final String PATIENT_1_CONDITION_RESOURCE_JSON = "{"
      + "  \"resourceType\": \"Condition\","
      + "  \"id\": \"c1\", "
      + "  \"subject\": {\"reference\": \"Patient/1\"}, "
      + "  \"code\": {\"coding\": ["
      + "    {\"system\": \"http://example.com/foosystem\", \"code\": \"3\"}"
      + "  ]}"
      + "}";

  private static final ZonedDateTime EVALUATION_TIME =
      ZonedDateTime.of(2022, 1, 7, 13, 14, 15, 0, ZoneOffset.UTC);

  private Path ndjsonFolder;
  private Path valueSetFolder;
  private Path cqlFolder;
  private Path resultsFolder;

  @Before
  public void setUp() throws IOException {
    ndjsonFolder = tempFolder.newFolder("ndjson").toPath();
    valueSetFolder = tempFolder.newFolder("valuesets").toPath();
    cqlFolder = tempFolder.newFolder("cql").toPath();
    resultsFolder = tempFolder.newFolder().toPath();
  }

  private static VersionedIdentifier versionedIdentifier(String id, String version) {
    VersionedIdentifier versionedIdentifier = new VersionedIdentifier();
    versionedIdentifier.setId(id);
    versionedIdentifier.setVersion(version);
    return versionedIdentifier;
  }

  private static Collection<CqlEvaluationResult> readResults(File folder) throws IOException {
    ReflectDatumReader<CqlEvaluationResult> reader =
        new ReflectDatumReader<>(CqlEvaluationResult.ResultCoder.SCHEMA);

    ArrayList<CqlEvaluationResult> results = new ArrayList<>();

    for (File file : folder.listFiles()) {
      try (DataFileReader<CqlEvaluationResult> fileReader =
          new DataFileReader<CqlEvaluationResult>(file, reader)) {
        fileReader.forEach(results::add);
      }
    }

    return results;
  }

  private void writeLinesToFile(Path path, String... content) throws IOException {
    Files.writeString(
        path,
        String.join("\n", content));
  }

  private Pipeline getTestPipeline(EvaluateCqlOptions options) {
    return testPipeline;
  }

  @Test
  public void assemblePipeline() throws IOException {
    writeLinesToFile(ndjsonFolder.resolve("resources1.ndjson"),
        PATIENT_1_RESOURCE_JSON, PATIENT_2_RESOURCE_JSON);
    writeLinesToFile(ndjsonFolder.resolve("resources2.ndjson"), PATIENT_1_CONDITION_RESOURCE_JSON);
    writeLinesToFile(valueSetFolder.resolve("valueset.json"),
        "{",
        "\"resourceType\": \"ValueSet\",",
        "\"url\": \"http://example.com/foovalueset\",",
        "\"expansion\": {",
        "  \"contains\": [",
        "    {",
        "      \"system\": \"http://example.com/foosystem\",",
        "      \"code\": \"3\"",
        "    }",
        "  ]",
        "  }",
        "}");
    writeLinesToFile(cqlFolder.resolve("foo.cql"),
        "library FooLibrary version '0.1'",
        "valueset \"FooSet\": 'http://example.com/foovalueset'",
        "codesystem \"FooSystem\": 'http://example.com/foosystem'",
        "code \"Code3\": '3' from \"FooSystem\"",
        "using FHIR version '4.0.1'",
        "context Patient",
        "define \"Exp1\": Count([Condition: \"FooSet\"]) > 0");

    String[] args = new String[]{
        "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
        "--valueSetFolder=" + valueSetFolder,
        "--cqlFolder=" + cqlFolder,
        "--cqlLibraries=[{\"name\": \"FooLibrary\"}]",
        "--outputFilenamePrefix=" + resultsFolder.resolve("output")};

    EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME);

    assertThat(readResults(resultsFolder.toFile()))
        .containsExactly(
            new CqlEvaluationResult(
                versionedIdentifier("FooLibrary", "0.1"),
                PATIENT_1_ID,
                EVALUATION_TIME,
                ImmutableMap.of("Exp1", new GenericExpressionValue(true))),
            new CqlEvaluationResult(
                versionedIdentifier("FooLibrary", "0.1"),
                PATIENT_2_ID,
                EVALUATION_TIME,
                ImmutableMap.of("Exp1", new GenericExpressionValue(false))));
  }

  @Test
  public void libraryNotFound() throws IOException {
    String[] args = new String[]{
        "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
        "--valueSetFolder=" + valueSetFolder,
        "--cqlFolder=" + cqlFolder,
        "--cqlLibraries=[{\"name\": \"BarLibrary\"}]",
        "--outputFilenamePrefix=" + resultsFolder.resolve("output")};

    Exception e = assertThrows(CqlTranslatorIncludeException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("Could not load source");

    testPipeline.enableAbandonedNodeEnforcement(false);
  }

  @Test
  public void libraryCompilationError() throws IOException {
    writeLinesToFile(cqlFolder.resolve("foo.cql"),
        "library FooLibrary version '0.1'",
        "BAD CQL");

    String[] args = new String[]{
        "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
        "--valueSetFolder=" + valueSetFolder,
        "--cqlFolder=" + cqlFolder,
        "--cqlLibraries=[{\"name\": \"FooLibrary\"}]",
        "--outputFilenamePrefix=" + resultsFolder.resolve("output")
    };

    Exception e = assertThrows(RuntimeException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("Syntax error");

    testPipeline.enableAbandonedNodeEnforcement(false);
  }

  @Test
  public void cqlLibrariesFlagWithEmptyList() {
    String[] args = new String[]{
        "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
        "--valueSetFolder=" + valueSetFolder,
        "--cqlFolder=" + cqlFolder,
        "--cqlLibraries=[]",
        "--outputFilenamePrefix=" + resultsFolder.resolve("output")
    };

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("At least one CQL library must be specified.");
  }

  @Test
  public void cqlLibrariesFlagNotSpecified() {
    String[] args = new String[]{
      "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
      "--valueSetFolder=" + valueSetFolder,
      "--cqlFolder=" + cqlFolder,
      "--outputFilenamePrefix=" + resultsFolder.resolve("output")
    };

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("--cqlLibraries");
  }

  @Test
  public void ndjsonFhirFilePatternFlagNotSpecified() {
    String[] args = new String[]{
      "--valueSetFolder=" + valueSetFolder,
      "--cqlFolder=" + cqlFolder,
      "--cqlLibraries=[]",
      "--outputFilenamePrefix=" + resultsFolder.resolve("output")
    };

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("--ndjsonFhirFilePattern");
  }

  @Test
  public void valueSetFolderFlagNotSpecified() {
    String[] args = new String[]{
      "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
      "--cqlFolder=" + cqlFolder,
      "--cqlLibraries=[]",
      "--outputFilenamePrefix=" + resultsFolder.resolve("output")
    };

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("--valueSetFolder");
  }

  @Test
  public void cqlFolderFlagNotSpecified() {
    String[] args = new String[]{
      "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
      "--valueSetFolder=" + valueSetFolder,
      "--cqlLibraries=[]",
      "--outputFilenamePrefix=" + resultsFolder.resolve("output")
    };

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("--cqlFolder");
  }

  @Test
  public void outputFilenamePrefixFlagNotSpecified() {
    String[] args = new String[]{
      "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
      "--valueSetFolder=" + valueSetFolder,
      "--cqlFolder=" + cqlFolder,
      "--cqlLibraries=[]"
    };

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.runPipeline(this::getTestPipeline, args, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("--outputFilenamePrefix");
  }
}
