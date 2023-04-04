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
package com.google.fhir.cql.beam.types;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/**
 * A container that holds a FHIR record's resource type and logical ID.
 */
@DefaultCoder(AvroCoder.class)
public final class ResourceTypeAndId implements Serializable {
  private String type;
  private String id;

  // Required for AvroCoder.
  public ResourceTypeAndId() {}

  public ResourceTypeAndId(String type, String id) {
    this.type = checkNotNull(type);
    this.id = checkNotNull(id);
  }

  /**
   * Returns the resource's type as a string (e.g. "Patient").
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the resource's logical ID.
   *
   * @see https://www.hl7.org/fhir/resource.html#id
   */
  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ResourceTypeAndId.class)
        .add("type", type)
        .add("id", id)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ResourceTypeAndId)) {
      return false;
    }

    ResourceTypeAndId that = (ResourceTypeAndId) other;

    return this.type.equals(that.type)
        && this.id.equals(that.id);
  }
}
