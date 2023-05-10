# Evaluating CQL with the Apache Beam SDK

This project contains code to evaluate [CQL](https://cql.hl7.org/) (Clinical
Quality Language) over large datasets by leveraging Apache Beam.

To maintain a narrow and focused scope there are various
limitations placed on CQL that can be evaluated. Specifically:

-   the pipeline only supports `Patient` contexts.
-   there is no support for accessing resources outside of the context (i.e. no
    support for cross-context and related context retrieves.)
-   all resources for a given patient must fit within the memory of a worker.
-   only R4 FHIR is supported.
-   only boolean expressions are computed and persisted in the pipeline's
    output.
-   parameters cannot be passed to the CQL libraries.

## Getting started

To get the pipeline up and running you will need:

-   A collection of R4 FHIR stored as NDJSON (new-line deliminted JSON). Sources
    of this include:
    -   The output of a bulk-data export operation as defined in the
        [Bulk Data Access IG].
    -   The output of invoking [`fhirStores.export`] on a Google Cloud
        Healthcare API's FHIR store. See also [Exporting FHIR resources].
    -   Generated synthetic data from [Synthea].
-   A CQL library with boolean expression that utilizes the `Patient` context.
-   All the required value sets persisted in FHIR JSON as [`ValueSet`]
    resources, one resource per file.

[Bulk Data Access IG]: https://hl7.org/fhir/uv/bulkdata/export/index.html
[Exporting FHIR resourcs]: https://cloud.google.com/healthcare-api/docs/how-tos/fhir-import-export#exporting_fhir_resources
[`fhirStores.export`]: https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/export
[Synthea]: https://github.com/synthetichealth/synthea#generate-synthetic-patients
[`ValueSet`]: http://hl7.org/fhir/R4/valueset.html

### Running locally

First, create some Synthetic data and grab some existing quality measures.

```
TMPDIR=$(mktemp -td cql-beam-local.XXXXX)

SYNTHEA_OUTPUT=$TMPDIR/synthea-ndjson
wget --directory-prefix=$TMPDIR https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
java -jar $TMPDIR/synthea-with-dependencies.jar \
  --exporter.fhir.use_us_core_ig true \
  --exporter.fhir.bulk_data true \
  --exporter.baseDirectory $SYNTHEA_OUTPUT \
  -p 10

git clone https://github.com/cqframework/ecqm-content-r4.git $TMPDIR/ecqm-content-r4

NDJSON_FHIR_FILE_PATTERN=$SYNTHEA_OUTPUT/fhir/*.ndjson
CQL_FOLDER=$TMPDIR/ecqm-content-r4/input/cql
VALUE_SET_FOLDER=$TMPDIR/ecqm-content-r4/input/vocabulary/valueset/external
OUTPUT_FILENAME_PREFIX=$TMPDIR/cql-output/output
```

Then build and run the pipeline. (Note: In order to utilize value sets and CQL
libraries stored in Google Cloud Storage you must use `mvn package` and then
execute the JAR with `java`, as is shown below in "Running with Google Cloud
Dataflow.")

```
mvn compile exec:java -e \
  -Dexec.args=" \
    --ndjsonFhirFilePattern='$NDJSON_FHIR_FILE_PATTERN' \
    --cqlFolder='$CQL_FOLDER' \
    --cqlLibraries='"'[
      {"name":"BreastCancerScreeningFHIR"},
      {"name":"CervicalCancerScreeningFHIR"},
      {"name":"ChlamydiaScreeningforWomenFHIR"},
      {"name":"ColorectalCancerScreeningsFHIR","version":"0.0.001"},
      {"name":"ControllingHighBloodPressureFHIR"},
      {"name":"DiabetesHemoglobinA1cHbA1cPoorControl9FHIR"},
      {"name":"DischargedonAntithromboticTherapyFHIR"}
    ]'"' \
    --valueSetFolder='$VALUE_SET_FOLDER' \
    --outputFilenamePrefix='$OUTPUT_FILENAME_PREFIX' \
    --streaming=true \
  "
```

You may then analyze the results with [Apache Drill](https://drill.apache.org/download/).

```
apache drill> SELECT * FROM dfs.`/tmp/cql-beam-local.*/cql-output/*.avro`;
```

### Running with Google Cloud Dataflow

Follow Google Cloud's Dataflow pipeline [Before you Begin] instructions. Once
complete, run the commands below from the base directory of this repository.
Replacing `<PROJECT_ID>` and `<BUCKET_NAME>` with the appropriate Google Cloud
project ID and Google Cloud Storage bucket, respectively.

[Before you Begin]: https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java#before-you-begin

```
CLOUD_PROJECT_ID=<PROJECT_ID>
CLOUD_REGION=us-west1
GCS_BUCKET=gs://<BUCKET_NAME>

TMPDIR=$(mktemp -td cql-beam-dataflow.XXXXX)

SYNTHEA_OUTPUT=$TMPDIR/synthea-ndjson
wget --directory-prefix=$TMPDIR https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
java -jar $TMPDIR/synthea-with-dependencies.jar \
  --exporter.fhir.use_us_core_ig true \
  --exporter.fhir.bulk_data true \
  --exporter.baseDirectory $SYNTHEA_OUTPUT \
  -p 10

git clone https://github.com/cqframework/ecqm-content-r4.git $TMPDIR/ecqm-content-r4

gsutil -m cp $SYNTHEA_OUTPUT/fhir/*.ndjson $GCS_BUCKET/fhir
gsutil -m cp $TMPDIR/ecqm-content-r4/input/cql/*.cql $GCS_BUCKET/cql
gsutil -m cp $TMPDIR/ecqm-content-r4/input/vocabulary/valueset/external/*.json $GCS_BUCKET/valuesets

NDJSON_FHIR_FILE_PATTERN=$GCS_BUCKET/fhir/*.ndjson
CQL_FOLDER=$GCS_BUCKET/cql
VALUE_SET_FOLDER=$GCS_BUCKET/valuesets
OUTPUT_FILENAME_PREFIX=$GCS_BUCKET/cql-output/output
```

Package and execute the resulting JAR.

```
mvn package

java -jar ./target/cql-beam-bundled-0.1.jar \
  --ndjsonFhirFilePattern="$NDJSON_FHIR_FILE_PATTERN" \
  --cqlFolder="$CQL_FOLDER" \
  --cqlLibraries='[
    {"name":"BreastCancerScreeningFHIR"},
    {"name":"CervicalCancerScreeningFHIR"},
    {"name":"ChlamydiaScreeningforWomenFHIR"},
    {"name":"ColorectalCancerScreeningsFHIR","version":"0.0.001"},
    {"name":"ControllingHighBloodPressureFHIR"},
    {"name":"DiabetesHemoglobinA1cHbA1cPoorControl9FHIR"},
    {"name":"DischargedonAntithromboticTherapyFHIR"}
  ]' \
  --valueSetFolder="$VALUE_SET_FOLDER" \
  --outputFilenamePrefix="$OUTPUT_FILENAME_PREFIX" \
  --runner=DataflowRunner \
  --gcpTempLocation=$GCS_BUCKET/tmp \
  --project=$CLOUD_PROJECT_ID \
  --usePublicIps=false \
  --region=$CLOUD_REGION
```

You may then analyze the results in BigQuery.

```
bq --location=$CLOUD_REGION mk --dataset $CLOUD_PROJECT_ID:cql_eval

bq load --source_format=AVRO --use_avro_logical_types \
  $CLOUD_PROJECT_ID:cql_eval.readme_example \
  $OUTPUT_FILENAME_PREFIX*

bq query --use_legacy_sql=false \
"
CREATE TEMP FUNCTION GetValue(
  expression_name STRING, results ARRAY<STRUCT<key STRING, value BOOL>>)
RETURNS bool
AS (
  (SELECT value FROM UNNEST(results) WHERE key = expression_name)
);

SELECT libraryId.name, libraryId.version,
  error,
  GetValue('Numerator', results) AS numerator,
  GetValue('Numerator Exclusions', results) AS numerator_exclusions,
  GetValue('Denominator', results) AS denominator,
  GetValue('Denominator Exclusions', results) AS denominator_exclusions,
  GetValue('Denominator Exceptions', results) AS denominator_exceptions,
  GetValue('Initial Population', results) AS initial_population,
  COUNT(0) AS count
FROM $CLOUD_PROJECT_ID.cql_eval.readme_example
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY libraryId.name ASC;
"
```

The output of the query should looking similar to the following:

|                    name                    | version | numerator | numerator_exclusions | denominator | denominator_exclusions | denominator_exceptions | initial_population | count |
|--------------------------------------------|---------|-----------|----------------------|-------------|------------------------|------------------------|--------------------|-------|
| BreastCancerScreeningFHIR                  | 2.0.003 |     false |                 NULL |       false |                   NULL |                   NULL |              false |    11 |
| ColorectalCancerScreeningsFHIR             | 0.0.001 |     false |                 NULL |       false |                   NULL |                   NULL |              false |     8 |
| ColorectalCancerScreeningsFHIR             | 0.0.001 |      true |                 NULL |       false |                   NULL |                   NULL |              false |     3 |
| ControllingHighBloodPressureFHIR           | 0.0.002 |      NULL |                 NULL |       false |                  false |                   NULL |              false |    11 |
| DiabetesHemoglobinA1cHbA1cPoorControl9FHIR | 0.0.001 |     false |                 NULL |       false |                  false |                   NULL |              false |     2 |
| DiabetesHemoglobinA1cHbA1cPoorControl9FHIR | 0.0.001 |      true |                 NULL |       false |                  false |                   NULL |              false |     9 |

## Feedback and getting help

You may create a [GitHub issue](https://github.com/google/cql-on-beam/issues)
for bugs and feature requests or, for more open ended conversations, start a
[GitHub discussion](https://github.com/google/cql-on-beam/discussions).

## Disclaimer

This is not an officially supported Google product and is for
demonstration purposes only.

FHIR® is the registered trademark of Health Level Seven International and use of
this trademarks does not constitute an endorsement by HL7.
