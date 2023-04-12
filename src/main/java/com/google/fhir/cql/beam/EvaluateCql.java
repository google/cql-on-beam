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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.fhir.cql.beam.types.CqlLibraryId;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Main entry point for evaluating CQL libraries over a set of FHIR records with Apache Beam.
 *
 * <p>See README.md for additional information.
 */
class EvaluateCql {
  /**
   * Options supported by {@link EvaluateCql}.
   */
  public interface EvaluateCqlOptions extends PipelineOptions {
    @Description(
        "The file pattern of the NDJSON FHIR files to read. Follows the conventions of "
            + "https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob.")
    @Required
    String getNdjsonFhirFilePattern();

    void setNdjsonFhirFilePattern(String value);

    @Description(
        "Path to a folder that contains ValueSet FHIR records. Each file must contain exactly one "
            + "ValueSet FHIR record. Subfolders and their content are ignored.")
    @Required
    String getValueSetFolder();

    void setValueSetFolder(String value);

    @Description(
        "Path to a folder that contains CQL libraries. Subfolders and their content are ignored.")
    @Required
    String getCqlFolder();

    void setCqlFolder(String value);

    @Description(
        "A list of CQL library IDs and, optionally, versions that will be evaluated "
            + "against the provided FHIR. Format: "
            + "[{\"name\": \"ColorectalCancerScreeningsFHIR\"}, "
            + "{\"name\": \"ControllingHighBloodPressureFHIR\" \"version\": \"0.0.002\"}]")
    @Required
    @JsonSerialize
    @JsonDeserialize
    List<CqlLibraryId> getCqlLibraries();

    void setCqlLibraries(List<CqlLibraryId> value);

    @Description("Path and name prefix of the file shards that will contain the pipeline output.")
    @Required
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String value);
  }

  @VisibleForTesting
  static void assemblePipeline(
      Pipeline pipeline, EvaluateCqlOptions options, ZonedDateTime evaluationDateTime) {
    checkArgument(
        !options.getCqlLibraries().isEmpty(), "At least one CQL library must be specified.");

    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  static EvaluateCqlOptions pipelineOptionsFromArgs(String... args) {
    return PipelineOptionsFactory.fromArgs(args).withValidation().as(EvaluateCqlOptions.class);
  }

  public static void main(String[] args) {
    EvaluateCqlOptions options = pipelineOptionsFromArgs(args);
    Pipeline pipeline = Pipeline.create(options);
    assemblePipeline(pipeline, options, ZonedDateTime.now());
    pipeline.run().waitUntilFinish();
  }
}
