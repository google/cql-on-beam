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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.fhir.cql.beam.types.CqlEvaluationResult;
import com.google.fhir.cql.beam.types.CqlLibraryId;
import com.google.fhir.cql.beam.types.GenericExpressionValue;
import com.google.fhir.cql.beam.types.MeasurementPeriod;
import com.google.fhir.cql.beam.types.ResourceTypeAndId;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.cqframework.cql.elm.execution.ExpressionDef;
import org.cqframework.cql.elm.execution.Library;
import org.cqframework.cql.elm.execution.ParameterDef;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Resource;
import org.opencds.cqf.cql.engine.data.CompositeDataProvider;
import org.opencds.cqf.cql.engine.execution.Context;
import org.opencds.cqf.cql.engine.execution.InMemoryLibraryLoader;
import org.opencds.cqf.cql.engine.execution.LibraryLoader;
import org.opencds.cqf.cql.engine.fhir.model.R4FhirModelResolver;
import org.opencds.cqf.cql.engine.model.ModelResolver;
import org.opencds.cqf.cql.engine.retrieve.RetrieveProvider;
import org.opencds.cqf.cql.engine.runtime.Date;
import org.opencds.cqf.cql.engine.runtime.DateTime;
import org.opencds.cqf.cql.engine.runtime.Interval;
import org.opencds.cqf.cql.engine.serializing.jackson.JsonCqlMapper;
import org.opencds.cqf.cql.engine.terminology.TerminologyProvider;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import org.opencds.cqf.cql.evaluator.engine.terminology.BundleTerminologyProvider;

/**
 * A function that evaluates a set of CQL libraries over the collection of resources, represented as
 * JSON strings, for a given context. The collection of resources should include every resource
 * necessary for evaluation of the CQL within the context (e.g., the entire Patient bundle).
 *
 * <p>The follow constraints currently exist:
 *
 * <ul>
 *   <li>there is no support for accessing resources outside of the context (i.e., no support for
 *       cross-context and related context retrieves).
 *   <li>all resources for the context must fit within the memory of a worker.
 *   <li>only boolean expressions are written to the resulting {@link CqlEvaluationResult} objects.
 *   <li>there is no support for passing parameters to the CQL libraries.
 * </ul>
 */
public final class EvaluateCqlForContextFn
    extends DoFn<KV<ResourceTypeAndId, Iterable<String>>, CqlEvaluationResult> {
  /** A context that disallows evaluation of cross-context CQL expressions. */
  private static class FixedContext extends Context {
    public FixedContext(
        Library library, ZonedDateTime evaluationZonedDateTime, ResourceTypeAndId contextValue) {
      super(library, evaluationZonedDateTime);
      super.setContextValue(contextValue.getType(), contextValue.getId());
      super.enterContext(contextValue.getType());
    }

    @Override
    public void enterContext(String context) {
      if (!context.equals(getCurrentContext())) {
        throw new IllegalStateException("Context switching is not supported.");
      }
      super.enterContext(context);
    }
  }

  /** A wrapper around {@link Library} that supports Java serialization. */
  private static class SerializableLibraryWrapper implements Serializable {
    private static JsonMapper jsonMapper =
        JsonCqlMapper.getMapper()
            .rebuild()
            .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
            .disable(SerializationFeature.INDENT_OUTPUT)
            .build();
    private Library library;

    public SerializableLibraryWrapper(Library library) {
      this.library = library;
    }

    public Library getLibrary() {
      return library;
    }

    private void readObject(ObjectInputStream inputStream) throws IOException {
      library = jsonMapper.readValue((InputStream) inputStream, Library.class);
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
      jsonMapper.writeValue((OutputStream) outputStream, library);
    }
  }

  private final ImmutableList<SerializableLibraryWrapper> cqlSources;
  private final ImmutableSet<CqlLibraryId> cqlLibraryIds;
  private final ImmutableList<String> valueSetJsonResources;
  private final ZonedDateTime evaluationDateTime;
  private final FhirVersionEnum fhirVersion;

  private transient ImmutableSet<VersionedIdentifier> cqlLibraryVersionedIdentifiers;
  private transient FhirContext fhirContext;
  private transient ModelResolver modelResolver;
  private transient TerminologyProvider terminologyProvider;
  private transient LibraryLoader libraryLoader;

  public EvaluateCqlForContextFn(
      Collection<Library> cqlSources,
      Set<CqlLibraryId> cqlLibraryIds,
      Collection<String> valueSetJsonResources,
      ZonedDateTime evaluationDateTime,
      FhirVersionEnum fhirVersion) {
    this.cqlSources =
        cqlSources.stream().map(SerializableLibraryWrapper::new).collect(toImmutableList());
    this.cqlLibraryIds = ImmutableSet.copyOf(cqlLibraryIds);
    this.valueSetJsonResources = ImmutableList.copyOf(valueSetJsonResources);
    this.evaluationDateTime = checkNotNull(evaluationDateTime);
    this.fhirVersion = checkNotNull(fhirVersion);
  }

  @DoFn.Setup
  public void setup() {
    cqlLibraryVersionedIdentifiers =
        cqlLibraryIds.stream()
            .map(id -> new VersionedIdentifier().withId(id.getName()).withVersion(id.getVersion()))
            .collect(toImmutableSet());
    fhirContext = new FhirContext(fhirVersion);
    modelResolver = new CachingModelResolver(new R4FhirModelResolver());
    terminologyProvider = createTerminologyProvider(fhirContext, valueSetJsonResources);
    libraryLoader = new InMemoryLibraryLoader(
        cqlSources.stream()
            .map(SerializableLibraryWrapper::getLibrary)
            .collect(Collectors.toList()));
  }

  @ProcessElement
  public void processElement(
      @Element KV<ResourceTypeAndId, Iterable<String>> contextResources,
      OutputReceiver<CqlEvaluationResult> out) {
    RetrieveProvider retrieveProvider = createRetrieveProvider(contextResources.getValue());
    for (VersionedIdentifier cqlLibraryId : cqlLibraryVersionedIdentifiers) {
      Library library = libraryLoader.load(cqlLibraryId);
      Context context = createContext(library, retrieveProvider, contextResources.getKey());
      MeasurementPeriod measurementPeriod = fetchMeasurementPeriod(library, context);
      try {
        out.output(new CqlEvaluationResult(
                library.getIdentifier(),
                contextResources.getKey(),
                evaluationDateTime,
                measurementPeriod,
                evaluate(library, context, contextResources.getKey())));
      } catch (Exception e) {
        out.output(
            new CqlEvaluationResult(
                library.getIdentifier(),
                contextResources.getKey(),
                evaluationDateTime,
                measurementPeriod,
                e));
      }
    }
  }

  private MeasurementPeriod fetchMeasurementPeriod(Library library, Context context) {
    if (library.getParameters() == null) {
      return new MeasurementPeriod();
    }

    for (ParameterDef parameter : library.getParameters().getDef()) {
      // Based on the assumption that normally measurement period parameter is named this way
      // in CQL scripts.
      if (parameter.getName().equals("MeasurementPeriod")
          || parameter.getName().equals("Measurement Period")) {
        Interval interval = (Interval) parameter.getDefault().evaluate(context);
        DateTime startDate = (DateTime) interval.getLow();
        DateTime endDate = (DateTime) interval.getHigh();
        // Truncating the exact time of the specified date as that is not needed.
        return new MeasurementPeriod(
            truncateDateTime(startDate).toString(), truncateDateTime(endDate).toString());
      }
    }
    return new MeasurementPeriod();
  }

  private Date truncateDateTime(DateTime dateTime) {
    OffsetDateTime offsetDateTime = dateTime.getDateTime();
    return new Date(
        offsetDateTime.getYear(), offsetDateTime.getMonthValue(), offsetDateTime.getDayOfMonth());
  }

  private Map<String, GenericExpressionValue> evaluate(
      Library library, Context context, ResourceTypeAndId contextValue) {
    HashMap<String, GenericExpressionValue> results = new HashMap<>();

    for (ExpressionDef expression : library.getStatements().getDef()) {
      if (!expression.getContext().equals(contextValue.getType())
          || !GenericExpressionValue.isSupportedExpression(expression)) {
        continue;
      }

      Object value = expression.evaluate(context);
      GenericExpressionValue genericVal = value != null ? GenericExpressionValue.from(value) : null;

      if (results.putIfAbsent(expression.getName(), genericVal) != null) {
        throw new InternalError("Duplicate expression name: " + expression.getName());
      }
    }
    return results;
  }

  private Context createContext(
      Library library, RetrieveProvider retrieveProvider, ResourceTypeAndId contextValue) {
    Context context = new FixedContext(library, evaluationDateTime, contextValue);
    context.setExpressionCaching(true);
    context.registerLibraryLoader(libraryLoader);
    context.registerTerminologyProvider(terminologyProvider);
    context.registerDataProvider(
        "http://hl7.org/fhir", new CompositeDataProvider(modelResolver, retrieveProvider));
    // TODO(nasha): Set user defined parameters via `context.setParameter`.
    return context;
  }

  private static TerminologyProvider createTerminologyProvider(
      FhirContext fhirContext, ImmutableList<String> valueSetJsonStrings) {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = new Bundle();
    bundle.setType(BundleType.COLLECTION);
    valueSetJsonStrings.stream()
        .map(parser::parseResource)
        .forEach((resource) -> bundle.addEntry().setResource((Resource) resource));
    return new BundleTerminologyProvider(fhirContext, bundle);
  }

  private RetrieveProvider createRetrieveProvider(Iterable<String> jsonResource) {
    Bundle bundle = new Bundle();
    bundle.setType(BundleType.COLLECTION);
    IParser parser = fhirContext.newJsonParser();
    jsonResource.forEach(
        element -> {
          bundle.addEntry().setResource((Resource) parser.parseResource(element));
        });
    BundleRetrieveProvider provider = new BundleRetrieveProvider(fhirContext, bundle);
    provider.setTerminologyProvider(terminologyProvider);
    provider.setExpandValueSets(true);
    return provider;
  }
}
