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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ResourceTypeAndId}. */
@RunWith(JUnit4.class)
public class ResourceTypeAndIdTest {
  @Test
  public void getType() {
    assertThat(new ResourceTypeAndId("Patient", "1").getType())
        .isEqualTo("Patient");
  }

  @Test
  public void getId() {
    assertThat(new ResourceTypeAndId("Patient", "1").getId())
        .isEqualTo("1");
  }

  @Test
  public void equals() {
    new EqualsTester()
      .addEqualityGroup(
          new ResourceTypeAndId("Patient", "1"),
          new ResourceTypeAndId("Patient", "1"))
      .addEqualityGroup(new ResourceTypeAndId("Patient", "2"))
      .addEqualityGroup(new ResourceTypeAndId("Encounter", "1"))
      .testEquals();
  }
}
