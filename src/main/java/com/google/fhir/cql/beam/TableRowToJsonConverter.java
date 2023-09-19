package com.google.fhir.cql.beam;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
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

  public static TableRowToJsonConverter from(String table, List<String> bigQueryTables) {
    Map<String, List<String>> tableToChoiceFieldsMap = buildTableToChoiceFieldsMap(bigQueryTables);
    return new TableRowToJsonConverter(table, tableToChoiceFieldsMap);
  }

  private TableRowToJsonConverter(
      String tableName, Map<String, List<String>> tableToChoiceFieldsMap) {
    this.tableName = tableName;
    this.tableToChoiceFieldsMap = tableToChoiceFieldsMap;
  }

  @ProcessElement
  public void processElement(@Element TableRow input, OutputReceiver<String> out) {
    JSONObject resourceJson =
        convertBigQueryChoiceFieldsToFhirJson(new JSONObject(new Gson().toJson(input)));

    resourceJson.put("resourceType", constructValidResourceName(this.tableName));

    out.output(resourceJson.toString());
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
}
