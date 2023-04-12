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

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.beam.sdk.testing.TestPipeline;
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

@Test
  public void cqlLibrariesFlagWithEmptyList() {
    EvaluateCql.EvaluateCqlOptions options = EvaluateCql.pipelineOptionsFromArgs(
        "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
        "--valueSetFolder=" + valueSetFolder,
        "--cqlFolder=" + cqlFolder,
        "--cqlLibraries=[]",
        "--outputFilenamePrefix=" + resultsFolder.resolve("output"));

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.assemblePipeline(testPipeline, options, EVALUATION_TIME));
    assertThat(e).hasMessageThat().contains("At least one CQL library must be specified.");
  }

  @Test
  public void cqlLibrariesFlagNotSpecified() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.pipelineOptionsFromArgs(
            "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
            "--valueSetFolder=" + valueSetFolder,
            "--cqlFolder=" + cqlFolder,
            "--outputFilenamePrefix=" + resultsFolder.resolve("output")));
    assertThat(e).hasMessageThat().contains("--cqlLibraries");
  }

  @Test
  public void ndjsonFhirFilePatternFlagNotSpecified() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.pipelineOptionsFromArgs(
            "--valueSetFolder=" + valueSetFolder,
            "--cqlFolder=" + cqlFolder,
            "--cqlLibraries=[]",
            "--outputFilenamePrefix=" + resultsFolder.resolve("output")));
    assertThat(e).hasMessageThat().contains("--ndjsonFhirFilePattern");
  }

  @Test
  public void valueSetFolderFlagNotSpecified() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.pipelineOptionsFromArgs(
            "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
            "--cqlFolder=" + cqlFolder,
            "--cqlLibraries=[]",
            "--outputFilenamePrefix=" + resultsFolder.resolve("output")));
    assertThat(e).hasMessageThat().contains("--valueSetFolder");
  }

  @Test
  public void cqlFolderFlagNotSpecified() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.pipelineOptionsFromArgs(
            "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
            "--valueSetFolder=" + valueSetFolder,
            "--cqlLibraries=[]",
            "--outputFilenamePrefix=" + resultsFolder.resolve("output")));
    assertThat(e).hasMessageThat().contains("--cqlFolder");
  }

  @Test
  public void outputFilenamePrefixFlagNotSpecified() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> EvaluateCql.pipelineOptionsFromArgs(
            "--ndjsonFhirFilePattern=" + ndjsonFolder + "/*",
            "--valueSetFolder=" + valueSetFolder,
            "--cqlFolder=" + cqlFolder,
            "--cqlLibraries=[]"));
    assertThat(e).hasMessageThat().contains("--outputFilenamePrefix");
  }
}
