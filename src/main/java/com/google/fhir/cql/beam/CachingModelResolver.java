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

import java.util.concurrent.ConcurrentHashMap;
import org.opencds.cqf.cql.engine.model.ModelResolver;

/**
 * A {@link ModelResolver} that caches the results of calls to {@link ModelResolver#resolveType}.
 */
public class CachingModelResolver extends ForwardingModelResolver {
  private final ConcurrentHashMap<String, Class<?>> typeCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Class<?>, Class<?>> objectTypeCache = new ConcurrentHashMap<>();

  public CachingModelResolver(ModelResolver resolver) {
    super(resolver);
  }

  @Override
  public Class<?> resolveType(String typeName) {
    return typeCache.computeIfAbsent(typeName, super::resolveType);
  }

  @Override
  public Class<?> resolveType(Object value) {
    return objectTypeCache.computeIfAbsent(
        value.getClass(),
        (key) -> {
          return super.resolveType(value);
        });
  }
}
