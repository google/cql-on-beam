package com.google.fhir.cql.beam;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.gson.Gson;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/**
 * Converts a TableRow object to its associated FHIR JSON string. The converter takes in a table
 * name which is used for adding "resourceType" field to the JSON string in order to make it a valid
 * FHIR JSON: https://build.fhir.org/json.html
 */
public class TableRowToJsonConverter extends DoFn<TableRow, String> {

  private final String tableName;
  private final Map<String, List<String>> tableToChoiceFieldsMap;
  // A list of paths we need to traverse to get to each timestamp field type.
  // For example:
  // {
  //   "meta": {
  //     "lastUpdated": "2018-07-05 17:45:13.576898 UTC"
  //   },
  //   "field1": {
  //     "field2": {
  //       "timestampField": "2018-07-05 17:45:13.576898 UTC"
  //     }
  //   }
  // }
  //
  // For the JSON structure above, the timestampFieldPaths would look like below:
  // [["meta", "lastUpdated"], ["field1", "field2", "timestampField"]]
  private final List<List<String>> timestampFieldPaths;

  public static TableRowToJsonConverter from(
      String table, Schema schema, List<String> bigQueryTables) {
    Map<String, List<String>> tableToChoiceFieldsMap = buildTableToChoiceFieldsMap(bigQueryTables);
    List<List<String>> timestampFieldPaths =
        extractTimestampFields(
            schema.getFields(), new ArrayList<List<String>>(), new ArrayList<String>());
    return new TableRowToJsonConverter(table, timestampFieldPaths, tableToChoiceFieldsMap);
  }

  private TableRowToJsonConverter(
      String tableName,
      List<List<String>> timestampFieldPaths,
      Map<String, List<String>> tableToChoiceFieldsMap) {
    this.tableName = tableName;
    this.tableToChoiceFieldsMap = tableToChoiceFieldsMap;
    this.timestampFieldPaths = timestampFieldPaths;
  }

  @ProcessElement
  public void processElement(@Element TableRow input, OutputReceiver<String> out) {
    JSONObject resourceJson = new JSONObject(new Gson().toJson(input));

    convertBigQueryTimestampFieldsToFhirInstant(resourceJson);

    convertBigQueryChoiceFieldsToFhirJson(resourceJson);

    resourceJson.put("resourceType", constructValidResourceName(this.tableName));

    out.output(resourceJson.toString());
  }

  private void convertBigQueryTimestampFieldsToFhirInstant(JSONObject resource) {
    for (List<String> fieldNames : timestampFieldPaths) {
      JSONObject parent = resource;
      for (int i = 0; i < fieldNames.size() - 1; i++) {
        parent = parent.getJSONObject(fieldNames.get(i));
      }

      Object timestampField = parent.get(fieldNames.get(fieldNames.size() - 1));
      String updatedValue =
          convertBigQueryTimestampToValidFHIRInstantString(timestampField.toString());
      parent.put(fieldNames.get(fieldNames.size() - 1), updatedValue);
    }
  }

  private String convertBigQueryTimestampToValidFHIRInstantString(String timestampField) {
    // Define a DateTimeFormatter for the input format.
    DateTimeFormatter inputFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS zzz");

    // Parse the input string into a ZonedDateTime.
    ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestampField, inputFormatter);

    // Define a DateTimeFormatter for the desired output format.
    DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    // Format the ZonedDateTime in the desired format
    return outputFormatter.format(zonedDateTime);
  }

  private JSONObject convertBigQueryChoiceFieldsToFhirJson(JSONObject resource) {
    List<String> choiceFields = this.tableToChoiceFieldsMap.get(this.tableName);
    for (String choiceField : choiceFields) {
      if (!resource.has(choiceField)) {
        continue;
      }
      JSONObject choiceFieldJson = resource.getJSONObject(choiceField);
      // We expect this to contain at most one element.
      // https://build.fhir.org/formats.html#choice
      Set<String> keySet = choiceFieldJson.keySet();
      if (keySet.isEmpty()) {
        continue;
      }
      String choiceKey = keySet.iterator().next();
      Object choiceValue = choiceFieldJson.get(choiceKey);
      String combinedKeys = choiceField + StringUtils.capitalize(choiceKey);
      resource.put(combinedKeys, choiceValue);
      resource.remove(choiceField);
    }
    return resource;
  }

  private static Map<String, List<String>> buildTableToChoiceFieldsMap(List<String> tableNames) {
    Map<String, List<String>> tableToChoiceFields = new HashMap<String, List<String>>();
    for (String tableName : tableNames) {
      List<String> choiceFields = getTableChoiceFields(tableName);
      tableToChoiceFields.put(tableName, choiceFields);
    }
    return tableToChoiceFields;
  }

  private static List<String> getTableChoiceFields(String tableName) {
    FhirContext ctxR4 = FhirContext.forR4();
    List<String> choiceFields = new ArrayList<String>();
    String resource = constructValidResourceName(tableName);
    for (BaseRuntimeChildDefinition childDef :
        ctxR4.getResourceDefinition(resource).getChildren()) {
      if (childDef instanceof RuntimeChildChoiceDefinition
          && !(childDef instanceof RuntimeChildExtension)) {
        RuntimeChildChoiceDefinition choiceField = (RuntimeChildChoiceDefinition) childDef;
        choiceFields.add(choiceField.getElementName());
      }
    }
    return choiceFields;
  }

  private static String constructValidResourceName(String tableName) {
    String[] tokens = tableName.split("_");
    String resourceName = "";
    for (String token : tokens) {
      resourceName += StringUtils.capitalize(token);
    }
    return resourceName;
  }

  private static List<List<String>> extractTimestampFields(
      FieldList fields, List<List<String>> timestampFieldsPaths, List<String> currFieldPath) {
    for (Field field : fields) {
      if (field.getType() == LegacySQLTypeName.RECORD) {
        currFieldPath.add(field.getName());
        extractTimestampFields(field.getSubFields(), timestampFieldsPaths, currFieldPath);
        currFieldPath.remove(field.getName());
      }
      if (field.getType() == LegacySQLTypeName.TIMESTAMP) {
        currFieldPath.add(field.getName());
        timestampFieldsPaths.add(new ArrayList<String>(currFieldPath));
        currFieldPath.remove(field.getName());
      }
    }
    return timestampFieldsPaths;
  }
}
