package com.google.fhir.cql.beam;

import com.google.api.services.bigquery.model.TableRow;
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

    PCollection<String> output =
        testPipeline
            .apply(Create.of(tableRow1, tableRow2))
            .apply(ParDo.of(TableRowToJsonConverter.from("patient", Arrays.asList("patient"))));

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

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "observation", Arrays.asList("patient", "observation"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"effectiveDateTime\":\"2018-07-05T17:45:13+00:00\",\"id\":\"observation_id_1\",\"valueQuantity\":{\"code\":\"fL\",\"system\":\"http://unitsofmeasure.org\"},\"resourceType\":\"Observation\"}");

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsTableRowToJsonWithMultipleTableNameParts() {
    TableRow tableRow =
        new TableRow().set("id", "medication_request_id_1").set("status", "COMPLETED");

    PCollection<String> output =
        testPipeline
            .apply(Create.of(Arrays.asList(tableRow)))
            .apply(
                ParDo.of(
                    TableRowToJsonConverter.from(
                        "medication_request",
                        Arrays.asList("patient", "observation", "medication_request"))));

    PAssert.that(output)
        .containsInAnyOrder(
            "{\"id\":\"medication_request_id_1\",\"status\":\"COMPLETED\",\"resourceType\":\"MedicationRequest\"}");

    testPipeline.run().waitUntilFinish();
  }
}
