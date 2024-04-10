package at.ac.uibk.dps.streamprocessingapplications.shared.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TaxiRide implements Serializable {
  private SenMLRecordString taxiIdentifier;
  private SenMLRecordString hackLicense;
  private SenMLRecordString pickupDatetime;
  private SenMLRecordDouble tripTimeInSecs;
  private SenMLRecordDouble tripDistance;
  private SenMLRecordDouble pickupLongitude;
  private SenMLRecordDouble pickupLatitude;
  private SenMLRecordDouble dropoffLongitude;
  private SenMLRecordDouble dropoffLatitude;
  private SenMLRecordString paymentType;
  private SenMLRecordDouble fareAmount;
  private SenMLRecordDouble surcharge;
  private SenMLRecordDouble mtaTax;
  private SenMLRecordDouble tipAmount;
  private SenMLRecordDouble tollsAmount;
  private SenMLRecordDouble totalAmount;

  // NOTE: These fields are to be annotated in the `etl` job
  private SenMLRecordString taxiCompany;
  private SenMLRecordString driverName;
  private SenMLRecordString taxiCity;

  public TaxiRide() {
    this.taxiIdentifier = new SenMLRecordString(null, "taxi_identifier", "string", null, null);
    this.hackLicense = new SenMLRecordString(null, "hack_license", "string", null, null);
    this.pickupDatetime = new SenMLRecordString(null, "hack_license", "time", null, null);
    this.tripTimeInSecs = new SenMLRecordDouble(null, "trip_time_in_secs", "s", null, null);
    this.tripDistance = new SenMLRecordDouble(null, "trip_distance", "m", null, null);
    this.pickupLongitude = new SenMLRecordDouble(null, "pickup_longitude", "deg", null, null);
    this.pickupLatitude = new SenMLRecordDouble(null, "pickup_latitude", "deg", null, null);
    this.dropoffLongitude = new SenMLRecordDouble(null, "dropoff_longitude", "deg", null, null);
    this.dropoffLatitude = new SenMLRecordDouble(null, "dropoff_latitude", "deg", null, null);
    this.paymentType = new SenMLRecordString(null, "payment_type", "payment_type", null, null);
    this.fareAmount = new SenMLRecordDouble(null, "fare_amount", "dollar", null, null);
    this.surcharge = new SenMLRecordDouble(null, "surcharge", "%", null, null);
    this.mtaTax = new SenMLRecordDouble(null, "mta_tax", "%", null, null);
    this.tipAmount = new SenMLRecordDouble(null, "tip_amount", "dollar", null, null);
    this.tollsAmount = new SenMLRecordDouble(null, "tolls_amount", "dollar", null, null);
    this.totalAmount = new SenMLRecordDouble(null, "total_amount", "dollar", null, null);
    this.taxiCompany = new SenMLRecordString(null, "taxi_company", "string", null, null);
    this.driverName = new SenMLRecordString(null, "driver_name", "string", null, null);
    this.taxiCity = new SenMLRecordString(null, "taxi_city", "string", null, null);
  }

  public TaxiRide(
      SenMLRecordString taxiIdentifier,
      SenMLRecordString hackLicense,
      SenMLRecordString pickupDatetime,
      SenMLRecordDouble tripTimeInSecs,
      SenMLRecordDouble tripDistance,
      SenMLRecordDouble pickupLongitude,
      SenMLRecordDouble pickupLatitude,
      SenMLRecordDouble dropoffLongitude,
      SenMLRecordDouble dropoffLatitude,
      SenMLRecordString paymentType,
      SenMLRecordDouble fareAmount,
      SenMLRecordDouble surcharge,
      SenMLRecordDouble mtaTax,
      SenMLRecordDouble tipAmount,
      SenMLRecordDouble tollsAmount,
      SenMLRecordDouble totalAmount) {
    this.taxiIdentifier = taxiIdentifier;
    this.hackLicense = hackLicense;
    this.pickupDatetime = pickupDatetime;
    this.tripTimeInSecs = tripTimeInSecs;
    this.tripDistance = tripDistance;
    this.pickupLongitude = pickupLongitude;
    this.pickupLatitude = pickupLatitude;
    this.dropoffLongitude = dropoffLongitude;
    this.dropoffLatitude = dropoffLatitude;
    this.paymentType = paymentType;
    this.fareAmount = fareAmount;
    this.surcharge = surcharge;
    this.mtaTax = mtaTax;
    this.tipAmount = tipAmount;
    this.tollsAmount = tollsAmount;
    this.totalAmount = totalAmount;
  }

  public Optional<String> getTaxiIdentifier() {
    return Optional.ofNullable(taxiIdentifier.getValue());
  }

  public void setTaxiIdentifier(String taxiIdentifier) {
    this.taxiIdentifier.setValue(taxiIdentifier);
  }

  public Optional<String> getHackLicense() {
    return Optional.ofNullable(hackLicense.getValue());
  }

  public void setHackLicense(String hackLicense) {
    this.hackLicense.setValue(hackLicense);
  }

  public Optional<String> getPickupDatetime() {
    return Optional.ofNullable(pickupDatetime.getValue());
  }

  public void setPickupDatetime(String pickupDatetime) {
    this.pickupDatetime.setValue(pickupDatetime);
  }

  public Optional<Double> getTripTimeInSecs() {
    return Optional.ofNullable(tripTimeInSecs.getValue());
  }

  public void setTripTimeInSecs(Double tripTimeInSecs) {
    this.tripTimeInSecs.setValue(tripTimeInSecs);
  }

  public Optional<Double> getTripDistance() {
    return Optional.ofNullable(tripDistance.getValue());
  }

  public void setTripDistance(Double tripDistance) {
    this.tripDistance.setValue(tripDistance);
  }

  public Optional<Double> getPickupLongitude() {
    return Optional.ofNullable(pickupLongitude.getValue());
  }

  public void setPickupLongitude(Double pickupLongitude) {
    this.pickupLongitude.setValue(pickupLongitude);
  }

  public Optional<Double> getPickupLatitude() {
    return Optional.ofNullable(pickupLatitude.getValue());
  }

  public void setPickupLatitude(Double pickupLatitude) {
    this.pickupLatitude.setValue(pickupLatitude);
  }

  public Optional<Double> getDropoffLongitude() {
    return Optional.ofNullable(dropoffLongitude.getValue());
  }

  public void setDropoffLongitude(Double dropoffLongitude) {
    this.dropoffLongitude.setValue(dropoffLongitude);
  }

  public Optional<Double> getDropoffLatitude() {
    return Optional.ofNullable(dropoffLatitude.getValue());
  }

  public void setDropoffLatitude(Double dropoffLatitude) {
    this.dropoffLatitude.setValue(dropoffLatitude);
  }

  public Optional<String> getPaymentType() {
    return Optional.ofNullable(paymentType.getValue());
  }

  public void setPaymentType(String paymentType) {
    this.paymentType.setValue(paymentType);
  }

  public Optional<Double> getFareAmount() {
    return Optional.ofNullable(fareAmount.getValue());
  }

  public void setFareAmount(Double fareAmount) {
    this.fareAmount.setValue(fareAmount);
  }

  public Optional<Double> getSurcharge() {
    return Optional.ofNullable(surcharge.getValue());
  }

  public void setSurcharge(double surcharge) {
    this.surcharge.setValue(surcharge);
  }

  public Optional<Double> getMtaTax() {
    return Optional.ofNullable(mtaTax.getValue());
  }

  public void setMtaTax(double mtaTax) {
    this.mtaTax.setValue(mtaTax);
  }

  public Optional<Double> getTipAmount() {
    return Optional.ofNullable(tipAmount.getValue());
  }

  public void setTipAmount(Double tipAmount) {
    this.tipAmount.setValue(tipAmount);
  }

  public Optional<Double> getTollsAmount() {
    return Optional.ofNullable(tollsAmount.getValue());
  }

  public void setTollsAmount(Double tollsAmount) {
    this.tollsAmount.setValue(tollsAmount);
  }

  public Optional<Double> getTotalAmount() {
    return Optional.ofNullable(totalAmount.getValue());
  }

  public void setTotalAmount(double totalAmount) {
    this.totalAmount.setValue(totalAmount);
  }

  public SenMLRecordString getTaxiCompany() {
    return taxiCompany;
  }

  public void setTaxiCompany(String company) {
    this.taxiCompany.setValue(company);
  }

  public SenMLRecordString getDriverName() {
    return driverName;
  }

  public void setDriverName(String name) {
    this.driverName.setValue(name);
  }

  public SenMLRecordString getTaxiCity() {
    return taxiCity;
  }

  public void setTaxiCity(String city) {
    this.taxiCity.setValue(city);
  }

  @Override
  public String toString() {
    List<String> nonNullFields = new ArrayList<>();

    Optional.ofNullable(taxiIdentifier).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(hackLicense).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(pickupDatetime).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(tripTimeInSecs).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(tripDistance).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(pickupLongitude).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(pickupLatitude).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(dropoffLongitude).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(dropoffLatitude).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(paymentType).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(fareAmount).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(surcharge).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(mtaTax).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(tipAmount).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(tollsAmount).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(totalAmount).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(taxiCompany).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(driverName).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(taxiCity).ifPresent(value -> nonNullFields.add(value.toString()));

    return "[" + String.join(",", nonNullFields) + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TaxiRide)) return false;
    TaxiRide taxiRide = (TaxiRide) o;
    return Objects.equals(this.taxiIdentifier, taxiRide.taxiIdentifier)
        && Objects.equals(this.hackLicense, taxiRide.hackLicense)
        && Objects.equals(this.pickupDatetime, taxiRide.pickupDatetime)
        && Objects.equals(this.tripTimeInSecs, taxiRide.tripTimeInSecs)
        && Objects.equals(this.tripDistance, taxiRide.tripDistance)
        && Objects.equals(this.pickupLongitude, taxiRide.pickupLongitude)
        && Objects.equals(this.pickupLatitude, taxiRide.pickupLatitude)
        && Objects.equals(this.dropoffLongitude, taxiRide.dropoffLongitude)
        && Objects.equals(this.dropoffLatitude, taxiRide.dropoffLatitude)
        && Objects.equals(this.paymentType, taxiRide.paymentType)
        && Objects.equals(this.fareAmount, taxiRide.fareAmount)
        && Objects.equals(this.surcharge, taxiRide.surcharge)
        && Objects.equals(this.mtaTax, taxiRide.mtaTax)
        && Objects.equals(this.tipAmount, taxiRide.tipAmount)
        && Objects.equals(this.tollsAmount, taxiRide.tollsAmount)
        && Objects.equals(this.totalAmount, taxiRide.totalAmount)
        && Objects.equals(this.taxiCompany, taxiRide.taxiCompany)
        && Objects.equals(this.driverName, taxiRide.driverName)
        && Objects.equals(this.taxiCity, taxiRide.taxiCompany);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getTaxiIdentifier(),
        getHackLicense(),
        getPickupDatetime(),
        getTripTimeInSecs(),
        getTripDistance(),
        getPickupLongitude(),
        getPickupLatitude(),
        getDropoffLongitude(),
        getDropoffLatitude(),
        getPaymentType(),
        getFareAmount(),
        getSurcharge(),
        getMtaTax(),
        getTipAmount(),
        getTollsAmount(),
        getTotalAmount(),
        getTaxiCompany(),
        getDriverName(),
        getTaxiCity());
  }
}
