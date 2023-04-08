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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSetMultimap.flatteningToImmutableSetMultimap;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.fhir.cql.beam.types.ResourceTypeAndId;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.hl7.elm_modelinfo.r1.ClassInfo;
import org.hl7.elm_modelinfo.r1.ModelInfo;
import org.hl7.elm_modelinfo.r1.RelationshipInfo;

/**
 * Function that keys FHIR resources represented in JSON by their CQL context (e.g., "Patient").
 *
 * <p>All references within the resources being processed must be relative (i.e., "Patient/123").
 *
 * @see https://cql.hl7.org/02-authorsguide.html#context
 */
public final class KeyForContextFn extends DoFn<String, KV<ResourceTypeAndId, String>> {
  private static final Pattern REFERENCE_PATTERN = Pattern.compile("([^/]+)/([^/]+)");
  private static final String REFERENCE_FIELD_NAME = "reference";
  private static final String SUBJECT_FIELD_NAME = "subject";
  private static final String PATIENT_FIELD_NAME = "patient";

  private final String context;
  private final ImmutableSetMultimap<String, String> relatedKeyElementByType;

  public KeyForContextFn(String context, ModelInfo modelInfo) {
    this.context = checkNotNull(context);
    this.relatedKeyElementByType = createRelatedKeyElementByTypeMap(context, modelInfo);
  }

  /**
   * Returns a map from resource type to related key fields for the provided context.
   */
  private static ImmutableSetMultimap<String, String> createRelatedKeyElementByTypeMap(
      String context, ModelInfo modelInfo) {
    return modelInfo.getTypeInfo().stream()
        .filter(typeInfo -> typeInfo instanceof ClassInfo)
        .map(typeInfo -> (ClassInfo) typeInfo)
        .collect(
            flatteningToImmutableSetMultimap(
                ClassInfo::getName,
                classInfo -> classInfo.getContextRelationship().stream()
                    .filter(relationship -> relationship.getContext().equals(context))
                    .map(RelationshipInfo::getRelatedKeyElement)));
  }

  @ProcessElement
  public void processElement(
      @Element String element, OutputReceiver<KV<ResourceTypeAndId, String>> out) {
    JsonObject resourceObject = JsonParser.parseString(element).getAsJsonObject();
    String resourceType = resourceObject.get("resourceType").getAsString();

    if (resourceType.equals(context)) {
      out.output(
          KV.of(
              new ResourceTypeAndId(
                  resourceObject.get("resourceType").getAsString(),
                  resourceObject.get("id").getAsString()),
              element));
    }

    relatedKeyElements(resourceType).stream()
        .map(fieldName -> asResourceTypeAndId(resourceObject, fieldName))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(typeAndId -> typeAndId.getType().equals(context))
        .forEach(typeAndId -> out.output(KV.of(typeAndId, element)));
  }

  private Set<String> relatedKeyElements(String resourceType) {
    Set<String> relatedKeyElements = new HashSet<>(relatedKeyElementByType.get(resourceType));

    // TODO(nasha): Remove this when ModelInfo contains a complete list of related key elements.
    // As is, it misses some resources so we make up for that by checking for the presence of
    // "subject" and "patient", two well know context fields.
    relatedKeyElements.add(SUBJECT_FIELD_NAME);
    relatedKeyElements.add(PATIENT_FIELD_NAME);

    return relatedKeyElements;
  }

  /**
   * Returns the resource type and ID of the FHIR reference located at {@code referenceFieldName}.
   */
  private Optional<ResourceTypeAndId> asResourceTypeAndId(
      JsonObject resourceObject, String referenceFieldName) {
    if (!resourceObject.has(referenceFieldName)) {
      return Optional.empty();
    }
    JsonElement referenceElement = resourceObject.get(referenceFieldName);
    if (!referenceElement.isJsonObject()) {
      // TODO(nasha): Support repeated fields (i.e. JsonArray).
      return Optional.empty();
    }
    JsonObject referenceObject = referenceElement.getAsJsonObject();
    if (!referenceObject.has(REFERENCE_FIELD_NAME)) {
      return Optional.empty();
    }
    return Optional.of(extractReference(referenceObject.get(REFERENCE_FIELD_NAME).getAsString()));
  }

  /**
   * @param referenceString a reference string in the format of "Type/ID" (e.g., "Patient/123")
   */
  private ResourceTypeAndId extractReference(String referenceString) {
    Matcher matcher = REFERENCE_PATTERN.matcher(referenceString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Unable to handle reference. Only relative references in the format of \"Type/ID\" are"
              + " supported.");
    }
    String referenceResourceType = matcher.group(1);
    String referenceResourceId = matcher.group(2);
    return new ResourceTypeAndId(referenceResourceType, referenceResourceId);
  }
}
