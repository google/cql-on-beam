package com.google.fhir.cql.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TableRowToJsonConverterTest {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void convertsTableRowToJson() {
    TableRow tableRow1 =
        new TableRow()
            .set("id", "patient_id_1")
            .set("gender", "male")
            .set("deceased", new TableRow().set("boolean", true));
    TableRow tableRow2 = new TableRow().set("id", "patient_id_2").set("gender", "female");

    Schema schema =
        Schema.of(
            FieldList.of(
                Field.of("id", LegacySQLTypeName.STRING),
                Field.of("gender", LegacySQLTypeName.STRING),
                Field.newBuilder(
                        "deceased",
                        LegacySQLTypeName.RECORD,
                        Field.of("boolean", LegacySQLTypeName.BOOLEAN))
                    .build()));

    PCollection<String> output =
        testPipeline
            .apply(Create.of(tableRow1, tableRow2))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from("patient", schema, Arrays.asList("patient"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"deceasedBoolean\":true,"
                + "\"gender\":\"male\",\"id\":\"patient_id_1\",\"resourceType\":\"Patient\"}",
            "{\"gender\":\"female\",\"id\":\"patient_id_2\",\"resourceType\":\"Patient\"}");

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsTableRowToJsonWithChoiceFields() {
    TableRow tableRow =
        new TableRow()
            .set("id", "observation_id_1")
            .set(
                "value",
                new TableRow()
                    .set(
                        "quantity",
                        new TableRow()
                            .set("code", "fL")
                            .set("system", "http://unitsofmeasure.org")))
            .set("effective", new TableRow().set("dateTime", "2018-07-05T17:45:13+00:00"));

    Schema schema =
        Schema.of(
            FieldList.of(
                Field.of("id", LegacySQLTypeName.STRING),
                Field.newBuilder(
                        "effective",
                        LegacySQLTypeName.RECORD,
                        Field.of("dateTime", LegacySQLTypeName.STRING))
                    .build(),
                Field.newBuilder(
                        "value",
                        LegacySQLTypeName.RECORD,
                        Field.newBuilder(
                                "quantity",
                                LegacySQLTypeName.RECORD,
                                Field.of("code", LegacySQLTypeName.STRING),
                                Field.of("system", LegacySQLTypeName.STRING))
                            .build())
                    .build()));

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "observation", schema, Arrays.asList("patient", "observation"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"effectiveDateTime\":\"2018-07-05T17:45:13+00:00\",\"id\":\"observation_id_1\",\"valueQuantity\":{\"code\":\"fL\",\"system\":\"http://unitsofmeasure.org\"},\"resourceType\":\"Observation\"}");

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsTableRowToJsonWithInstantField() {
    TableRow tableRow =
        new TableRow()
            .set("id", "observation_id_1")
            .set("lastUpdated", "2023-10-12 18:04:49.578583 UTC");

    Schema schema =
        Schema.of(
            FieldList.of(
                Field.of("id", LegacySQLTypeName.STRING),
                Field.of("lastUpdated", LegacySQLTypeName.TIMESTAMP)));

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "observation", schema, Arrays.asList("patient", "observation"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"lastUpdated\":\"2023-10-12T18:04:49.578Z\",\"id\":\"observation_id_1\",\"resourceType\":\"Observation\"}");

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsTableRowToJsonWithNestedInstantFields() {
    TableRow tableRow =
        new TableRow()
            .set("id", "observation_id_1")
            .set("meta", new TableRow().set("lastUpdated", "2023-10-12 18:04:49.578583 UTC"))
            .set("effective", new TableRow().set("dateTime", "2018-07-05 17:45:13.576898 UTC"));

    Schema schema =
        Schema.of(
            FieldList.of(
                Field.of("id", LegacySQLTypeName.STRING),
                Field.newBuilder(
                        "meta",
                        LegacySQLTypeName.RECORD,
                        Field.of("lastUpdated", LegacySQLTypeName.TIMESTAMP))
                    .build(),
                Field.newBuilder(
                        "effective",
                        LegacySQLTypeName.RECORD,
                        Field.of("dateTime", LegacySQLTypeName.TIMESTAMP))
                    .build()));

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "observation", schema, Arrays.asList("patient", "observation"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"effectiveDateTime\":\"2018-07-05T17:45:13.576Z\",\"meta\":{\"lastUpdated\":\"2023-10-12T18:04:49.578Z\"},\"id\":\"observation_id_1\",\"resourceType\":\"Observation\"}");

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsTableRowToJsonWithDeepNestedInstantFields() {
    TableRow tableRow =
        new TableRow()
            .set("id", "observation_id_1")
            .set(
                "parent1",
                new TableRow()
                    .set(
                        "parent2",
                        new TableRow().set("parent3", "2023-10-12 18:04:49.578583 UTC")));

    Schema schema =
        Schema.of(
            FieldList.of(
                Field.of("id", LegacySQLTypeName.STRING),
                Field.newBuilder(
                        "parent1",
                        LegacySQLTypeName.RECORD,
                        Field.newBuilder(
                                "parent2",
                                LegacySQLTypeName.RECORD,
                                Field.of("parent3", LegacySQLTypeName.TIMESTAMP))
                            .build())
                    .build()));

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "observation", schema, Arrays.asList("patient", "observation"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"parent1\":{\"parent2\":"
                + "{\"parent3\":\"2023-10-12T18:04:49.578Z\"}},\"id\":\"observation_id_1\",\"resourceType\":\"Observation\"}");

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsTableRowToJsonWithMultipleTableNameParts() {
    TableRow tableRow =
        new TableRow().set("id", "medication_request_id_1").set("status", "COMPLETED");

    Schema schema =
        Schema.of(
            FieldList.of(
                Field.of("id", LegacySQLTypeName.STRING),
                Field.of("status", LegacySQLTypeName.STRING)));

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "medication_request",
                        schema,
                        Arrays.asList("patient", "observation", "medication_request"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"id\":\"medication_request_id_1\",\"status\":\"COMPLETED\",\"resourceType\":\"MedicationRequest\"}");

    testPipeline.run().waitUntilFinish();
  }
}
