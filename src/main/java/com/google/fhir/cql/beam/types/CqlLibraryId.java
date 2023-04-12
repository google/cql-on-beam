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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.cqframework.cql.elm.execution.VersionedIdentifier;

/** A CQL library identifier that can be serialized by {@link AvroCoder}. */
@DefaultCoder(AvroCoder.class)
public final class CqlLibraryId implements Serializable {
  private String name;

  @Nullable
  private String version;

  // Required for AvroCoder.
  public CqlLibraryId() {}

  public CqlLibraryId(VersionedIdentifier cqlLibraryIdentifier) {
    this(cqlLibraryIdentifier.getId(), cqlLibraryIdentifier.getVersion());
  }

  @JsonCreator
  public CqlLibraryId(
      @JsonProperty("name") String name,
      @Nullable @JsonProperty("version") String version) {
    this.name = checkNotNull(name);
    this.version = version;
  }

  /**
   * Returns the library's name.
   *
   * @see https://cql.hl7.org/02-authorsguide.html#library
   */
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  /**
   * Returns the library's version.
   *
   * @see https://cql.hl7.org/02-authorsguide.html#library
   */
  @Nullable
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(CqlLibraryId.class)
        .add("name", name)
        .add("version", version)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CqlLibraryId)) {
      return false;
    }

    CqlLibraryId that = (CqlLibraryId) other;

    return this.name.equals(that.name) && Objects.equals(this.version, that.version);
  }
}
