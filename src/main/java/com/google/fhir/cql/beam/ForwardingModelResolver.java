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

import org.opencds.cqf.cql.engine.model.ModelResolver;

/** A {@link ModelResolver} that forwards calls to the provided {@code ModelResolver}. */
public abstract class ForwardingModelResolver implements ModelResolver {
  private final ModelResolver resolver;

  public ForwardingModelResolver(ModelResolver resolver) {
    this.resolver = resolver;
  }

  @SuppressWarnings("deprecation")
  @Override
  public String getPackageName() {
    return resolver.getPackageName();
  }

  @SuppressWarnings("deprecation")
  @Override
  public void setPackageName(String packageName) {
    resolver.setPackageName(packageName);
  }

  @Override
  public Object resolvePath(Object target, String path) {
    return resolver.resolvePath(target, path);
  }

  @Override
  public Object getContextPath(String contextType, String targetType) {
    return resolver.getContextPath(contextType, targetType);
  }

  @Override
  public Class<?> resolveType(String typeName) {
    return resolver.resolveType(typeName);
  }

  @Override
  public Class<?> resolveType(Object value) {
    return resolver.resolveType(value);
  }

  @Override
  public Boolean is(Object value, Class<?> type) {
    return resolver.is(value, type);
  }

  @Override
  public Object as(Object value, Class<?> type, boolean isStrict) {
    return resolver.as(value, type, isStrict);
  }

  @Override
  public Object createInstance(String typeName) {
    return resolver.createInstance(typeName);
  }

  @Override
  public void setValue(Object target, String path, Object value) {
    resolver.setValue(target, path, value);
  }

  @Override
  public Boolean objectEqual(Object left, Object right) {
    return resolver.objectEqual(left, right);
  }

  @Override
  public Boolean objectEquivalent(Object left, Object right) {
    return resolver.objectEquivalent(left, right);
  }
}
