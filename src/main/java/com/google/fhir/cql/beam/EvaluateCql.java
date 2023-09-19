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
import static com.google.common.collect.ImmutableList.toImmutableList;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.fhir.cql.beam.types.CqlEvaluationResult;
import com.google.fhir.cql.beam.types.CqlLibraryId;
import com.google.fhir.cql.beam.types.ResourceTypeAndId;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.cqframework.cql.cql2elm.CqlCompilerException;
import org.cqframework.cql.cql2elm.CqlCompilerException.ErrorSeverity;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions.Options;
import org.cqframework.cql.cql2elm.LibraryManager;
import org.cqframework.cql.cql2elm.ModelManager;
import org.cqframework.cql.cql2elm.model.CompiledLibrary;
import org.cqframework.cql.elm.execution.Library;
import org.opencds.cqf.cql.evaluator.cql2elm.content.InMemoryLibrarySourceProvider;
import org.opencds.cqf.cql.evaluator.engine.elm.LibraryMapper;

/**
 * Main entry point for evaluating CQL libraries over a set of FHIR records with Apache Beam.
 *
 * <p>See README.md for additional information.
 */
public final class EvaluateCql {
  /** Options supported by {@link EvaluateCql}. */
  public interface EvaluateCqlOptions extends PipelineOptions {

    /** Creates the default list of BigQuery tables to run the pipeline on. */
    class TableListDefault implements DefaultValueFactory<List<String>> {
      @Override
      public List<String> create(PipelineOptions options) {
        return Arrays.asList(
            "patient",
            "condition",
            "allergy_intolerance",
            "basic",
            "care_plan",
            "claim",
            "diagnostic_report",
            "encounter",
            "explanation_of_benefit",
            "goal",
            "imaging_study",
            "immunization",
            "medication_request",
            "observation",
            "organization",
            "practitioner",
            "procedure");
      }
    }

    @Description(
        "The file pattern of the NDJSON FHIR files to read. Follows the conventions of "
            + "https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob.")
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

    @Description("Specifies whether to read from BigQuery.")
    @Default.Boolean(false)
    Boolean getReadFromBigQuery();

    void setReadFromBigQuery(Boolean value);

    @Description("The BigQuery project where the dataset to read from is located.")
    String getBigQueryProjectName();

    void setBigQueryProjectName(String value);

    @Description("The BigQuery dataset to read from.")
    String getDatasetName();

    void setDatasetName(String value);

    @Description("The set of tables to read from the BigQuery dataset.")
    @Default.InstanceFactory(TableListDefault.class)
    List<String> getBigQueryTables();

    void setBigQueryTables(List<String> value);
  }

  private static ImmutableList<String> loadFilesInDirectory(
      Path directory, Predicate<Path> pathFilter) {
    try {
      return FileLoader.loadFilesInDirectory(directory, pathFilter);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read files in " + directory, e);
    }
  }

  private static Path toPath(String directory) {
    try {
      URI directoryUri = new URI(directory);
      if (new URI(directory).getScheme() != null) {
        return Paths.get(directoryUri);
      }
    } catch (URISyntaxException e) {
      // Fall through and treat as a file:// directory.
    }

    return new File(directory).toPath();
  }

  private static final Options[] TRANSLATOR_OPTIONS = {
    Options.DisableListPromotion,
    Options.DisableListDemotion,
    Options.EnableResultTypes,
    Options.EnableLocators
  };

  private static ImmutableList<Library> loadLibraries(
      Path cqlFolder, Collection<CqlLibraryId> cqlLibraryIds) {
    LibraryManager libraryManager = new LibraryManager(new ModelManager());
    libraryManager
        .getLibrarySourceLoader()
        .registerProvider(
            new InMemoryLibrarySourceProvider(
                loadFilesInDirectory(cqlFolder, (path) -> path.toString().endsWith(".cql"))));
    libraryManager.enableCache();

    for (CqlLibraryId libraryIds : cqlLibraryIds) {
      List<CqlCompilerException> errors = new ArrayList<>();
      libraryManager.resolveLibrary(
          new org.hl7.elm.r1.VersionedIdentifier()
              .withId(libraryIds.getName())
              .withVersion(libraryIds.getVersion()),
          new CqlTranslatorOptions(TRANSLATOR_OPTIONS),
          errors);
      if (errors.stream().filter(error -> error.getSeverity().equals(ErrorSeverity.Error)).count()
          > 0) {
        throw new RuntimeException(
            "Errors encountered while compiling CQL. " + errors.toString());
      }
    }

    return libraryManager.getCompiledLibraries().values().stream()
        .map(CompiledLibrary::getLibrary)
        .map(LibraryMapper.INSTANCE::map)
        .collect(toImmutableList());
  }

  private static PCollection<String> fetchSourceData(
      Pipeline pipeline, EvaluateCqlOptions options) {
    if (!options.getReadFromBigQuery()) {
      checkArgument(
          !(options.getNdjsonFhirFilePattern() == null),
          "NDJSON FHIR files path must be specified if not reading from BigQuery.");
      return pipeline.apply("ReadNDJSON", TextIO.read().from(options.getNdjsonFhirFilePattern()));
    }

    checkArgument(
        !(options.getBigQueryProjectName() == null),
        "BigQuery project must be specified when reading from BigQuery.");

    checkArgument(
        !(options.getDatasetName() == null),
        "BigQuery dataset must be specified when reading from BigQuery.");

    PCollectionList<String> allTableRows = PCollectionList.<String>empty(pipeline);
    for (String table : options.getBigQueryTables()) {
      PCollection<String> tableRows =
          pipeline
              .apply(
                  "ReadBigQueryTableRows",
                  BigQueryIO.readTableRows()
                      .from(
                          String.format(
                              "%s:%s.%s",
                              options.getBigQueryProjectName(), options.getDatasetName(), table))
                      .withMethod(TypedRead.Method.DIRECT_READ))
              .apply(
                  "ConvertTableRowToJson",
                  ParDo.of(TableRowToJsonConverter.from(table, options.getBigQueryTables())));

      allTableRows = allTableRows.and(tableRows);
    }
    // Merges the different PCollection of JSON strings into a single logical PCollection.
    return allTableRows.apply(Flatten.pCollections());
  }

  private static void assemblePipeline(
      Pipeline pipeline, EvaluateCqlOptions options, ZonedDateTime evaluationDateTime) {
    checkArgument(
        !options.getCqlLibraries().isEmpty(), "At least one CQL library must be specified.");

    PCollection<String> sourceData = fetchSourceData(pipeline, options);

    sourceData
        .apply(
            "KeyForContext",
            ParDo.of(
                new KeyForContextFn(
                    "Patient", new ModelManager().resolveModel("FHIR", "4.0.1").getModelInfo())))
        .apply("GroupByContext", GroupByKey.<ResourceTypeAndId, String>create())
        .apply(
            "EvaluateCql",
            ParDo.of(
                new EvaluateCqlForContextFn(
                    loadLibraries(toPath(options.getCqlFolder()), options.getCqlLibraries()),
                    ImmutableSet.copyOf(options.getCqlLibraries()),
                    loadFilesInDirectory(
                        toPath(options.getValueSetFolder()),
                        (path) -> path.toString().endsWith(".json")),
                    evaluationDateTime,
                    FhirVersionEnum.R4)))
        .apply(
            "WriteCqlOutput",
            AvroIO.write(CqlEvaluationResult.class)
                .withSchema(CqlEvaluationResult.ResultCoder.SCHEMA)
                .to(options.getOutputFilenamePrefix())
                .withSuffix(".avro"));
  }

  @VisibleForTesting
  static void runPipeline(
      Function<EvaluateCqlOptions, Pipeline> pipelineCreator,
      String[] args,
      ZonedDateTime evaluationDateTime) {
    EvaluateCqlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(EvaluateCqlOptions.class);
    Pipeline pipeline = pipelineCreator.apply(options);
    assemblePipeline(pipeline, options, evaluationDateTime);
    pipeline.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    runPipeline(Pipeline::create, args, ZonedDateTime.now());
  }
}
