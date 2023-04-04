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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.testing.EqualsTester;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CqlLibraryId}. */
@RunWith(JUnit4.class)
public class CqlLibraryIdTest {
  @Test
  public void getName() {
    assertThat(new CqlLibraryId("Foo", "1").getName())
        .isEqualTo("Foo");
  }

  @Test
  public void getVersion() {
    assertThat(new CqlLibraryId("Foo", "1").getVersion())
        .isEqualTo("1");
  }

  @Test
  public void equals() {
    new EqualsTester()
      .addEqualityGroup(
          new CqlLibraryId("Foo", "1.0"),
          new CqlLibraryId("Foo", "1.0"),
          new CqlLibraryId(new VersionedIdentifier().withId("Foo").withVersion("1.0")))
      .addEqualityGroup(
          new CqlLibraryId("Bar", null),
          new CqlLibraryId(new VersionedIdentifier().withId("Bar")))
      .addEqualityGroup(new CqlLibraryId("Foo", "2.0"))
      .addEqualityGroup(new CqlLibraryId("Bar", "1.0"))
      .testEquals();
  }
}
