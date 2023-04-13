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
import static org.apache.avro.LogicalTypes.timestampMillis;
import static org.apache.avro.SchemaBuilder.builder;
import static org.apache.avro.SchemaBuilder.unionOf;

import autovalue.shaded.org.jetbrains.annotations.Nullable;
import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.cqframework.cql.elm.execution.VersionedIdentifier;

/**
 * A container that holds the results of evaluating a specific CQL library in a specific context.
 */
@DefaultCoder(CqlEvaluationResult.ResultCoder.class)
public final class CqlEvaluationResult {
  /**
   * A custom {@link AvroCoder} for {@link CqlEvaluationResult} objects.
   *
   * <p>This coder uses a custom Avro schema rather than the one generated via reflection due to the
   * results map need to support nullable values, which is not possible when using Avro's reflection
   * based schema generation.
   */
  public static class ResultCoder extends AvroCoder<CqlEvaluationResult> {
    public ResultCoder() {
      super(CqlEvaluationResult.class, SCHEMA, true);
    }

    /** The schema for {@link CqlEvaluationResult}. */
    public static Schema SCHEMA = builder(CqlEvaluationResult.class.getPackageName())
        .record(CqlEvaluationResult.class.getSimpleName()).fields()
            .name("contextId").type(schemaFor(ResourceTypeAndId.class)).noDefault()
            .name("libraryId").type(schemaFor(CqlLibraryId.class)).noDefault()
            .name("evaluationTime")
                .type(timestampMillis().addToSchema(builder().longType()))
                .noDefault()
            .name("error").type(unionOf().nullType().and().stringType().endUnion()).noDefault()
            .name("results")
                .type(unionOf()
                    .nullType().and()
                    .map().values().unionOf().nullType().and().booleanType().endUnion()
                    .endUnion())
                .noDefault()
        .endRecord();

    private static Schema schemaFor(Class<?> clazz) {
      return builder(clazz.getPackageName()).type(AvroCoder.of(clazz).getSchema());
    }

    @SuppressWarnings("unchecked")
    public static CoderProvider getCoderProvider() {
      return new CoderProvider() {
        @Override
        public <T> Coder<T> coderFor(
            TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
            throws CannotProvideCoderException {
          if (typeDescriptor.getRawType() != CqlEvaluationResult.class) {
            throw new CannotProvideCoderException(typeDescriptor + " is not supported");
          }
          return (Coder<T>) new ResultCoder();
        }
      };
    }
  }

  private CqlLibraryId libraryId;
  private ResourceTypeAndId contextId;
  private long evaluationTime;
  @Nullable private String error;
  @Nullable private Map<String, Boolean> results;

  // Required for AvroCoder.
  public CqlEvaluationResult() {}

  public CqlEvaluationResult(
      VersionedIdentifier cqlLibraryIdentifier,
      ResourceTypeAndId contextId,
      ZonedDateTime evaluationDateTime,
      Map<String, Boolean> results) {
    this.libraryId = new CqlLibraryId(cqlLibraryIdentifier);
    this.contextId = checkNotNull(contextId);
    this.evaluationTime = evaluationDateTime.toInstant().toEpochMilli();
    this.results = Collections.unmodifiableMap(new HashMap<>(results));
  }

  public CqlEvaluationResult(
      VersionedIdentifier cqlLibraryIdentifier,
      ResourceTypeAndId contextId,
      ZonedDateTime evaluationDateTime,
      Exception exception) {
    this.libraryId = new CqlLibraryId(cqlLibraryIdentifier);
    this.contextId = checkNotNull(contextId);
    this.evaluationTime = evaluationDateTime.toInstant().toEpochMilli();
    this.error = exception.getMessage();
  }

  public CqlLibraryId getLibraryId() {
    return libraryId;
  }

  public ResourceTypeAndId getContexId() {
    return contextId;
  }

  public Instant getEvaluationTime() {
    return Instant.ofEpochMilli(evaluationTime);
  }

  public String getError() {
    return error;
  }

  public Map<String, Boolean> getResults() {
    return results;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(CqlEvaluationResult.class)
        .add("libraryId", libraryId)
        .add("contextId", contextId)
        .add("evaluationTime", evaluationTime)
        .add("results", results)
        .add("error", error)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(libraryId, contextId, evaluationTime, results, error);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CqlEvaluationResult)) {
      return false;
    }
    CqlEvaluationResult that = (CqlEvaluationResult) other;
    return this.libraryId.equals(that.libraryId)
        && this.contextId.equals(that.contextId)
        && this.evaluationTime == that.evaluationTime
        && Objects.equals(this.results, that.results)
        && Objects.equals(this.error, that.error);
  }
}
