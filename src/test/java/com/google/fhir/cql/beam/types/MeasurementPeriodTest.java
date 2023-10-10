package com.google.fhir.cql.beam.types;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.testing.EqualsTester;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class MeasurementPeriodTest {

  @Test
  public void getStartDate() {
    MeasurementPeriod measurementPeriod = new MeasurementPeriod("2019-01-01", "2020-01-01");

    assertThat(measurementPeriod.getStartDate()).isEqualTo("2019-01-01");
  }

  @Test
  public void getEndDate() {
    MeasurementPeriod measurementPeriod = new MeasurementPeriod("2019-01-01", "2020-01-01");

    assertThat(measurementPeriod.getEndDate()).isEqualTo("2020-01-01");
  }

  @Test
  public void equals() {
    new EqualsTester()
        .addEqualityGroup(
            new MeasurementPeriod("2019-01-01", "2020-01-01"),
            new MeasurementPeriod("2019-01-01", "2020-01-01"))
        .addEqualityGroup(new MeasurementPeriod("2022-01-01", "2023-01-01"))
        .testEquals();
  }
}
