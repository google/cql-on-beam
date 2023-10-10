package com.google.fhir.cql.beam.types;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/**
 * A container that holds the start and end date of the measurement period. The assumption is that
 * the measurement period dates specified are inclusive. MeasurementPeriod encodes the dates as
 * strings for Avro to be able to serialize/deserialize them.
 */
@DefaultCoder(AvroCoder.class)
public final class MeasurementPeriod implements Serializable {
  @Nullable private String startDate;
  @Nullable private String endDate;

  // Required for AvroCoder.
  public MeasurementPeriod() {}

  public MeasurementPeriod(String startDate, String endDate) {
    this.startDate = startDate;
    this.endDate = endDate;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(MeasurementPeriod.class)
        .add("startDate", startDate)
        .add("endDate", endDate)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.startDate, this.endDate);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MeasurementPeriod)) {
      return false;
    }

    MeasurementPeriod that = (MeasurementPeriod) other;

    return Objects.equals(this.startDate, that.startDate)
        && Objects.equals(this.endDate, that.endDate);
  }
}
