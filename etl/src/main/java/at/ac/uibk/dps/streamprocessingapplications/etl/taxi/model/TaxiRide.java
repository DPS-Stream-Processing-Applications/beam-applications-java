package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordDouble;
import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordString;

public class TaxiRide {
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

  public TaxiRide() {}

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

  public String getTaxiIdentifier() {
    return taxiIdentifier.getValue();
  }

  public void setTaxiIdentifier(String taxiIdentifier) {
    this.taxiIdentifier.setValue(taxiIdentifier);
  }

  public String getHackLicense() {
    return hackLicense.getValue();
  }

  public void setHackLicense(String hackLicense) {
    this.hackLicense.setValue(hackLicense);
  }

  public String getPickupDatetime() {
    return pickupDatetime.getValue();
  }

  public void setPickupDatetime(String pickupDatetime) {
    this.pickupDatetime.setValue(pickupDatetime);
  }

  public double getTripTimeInSecs() {
    return tripTimeInSecs.getValue();
  }

  public void setTripTimeInSecs(double tripTimeInSecs) {
    this.tripTimeInSecs.setValue(tripTimeInSecs);
  }

  public double getTripDistance() {
    return tripDistance.getValue();
  }

  public void setTripDistance(double tripDistance) {
    this.tripDistance.setValue(tripDistance);
  }

  public Double getPickupLongitude() {
    return pickupLongitude.getValue();
  }

  public void setPickupLongitude(Double pickupLongitude) {
    this.pickupLongitude.setValue(pickupLongitude);
  }

  public Double getPickupLatitude() {
    return pickupLatitude.getValue();
  }

  public void setPickupLatitude(Double pickupLatitude) {
    this.pickupLatitude.setValue(pickupLatitude);
  }

  public Double getDropoffLongitude() {
    return dropoffLongitude.getValue();
  }

  public void setDropoffLongitude(Double dropoffLongitude) {
    this.dropoffLongitude.setValue(dropoffLongitude);
  }

  public Double getDropoffLatitude() {
    return dropoffLatitude.getValue();
  }

  public void setDropoffLatitude(Double dropoffLatitude) {
    this.dropoffLatitude.setValue(dropoffLatitude);
  }

  public String getPaymentType() {
    return paymentType.getValue();
  }

  public void setPaymentType(String paymentType) {
    this.paymentType.setValue(paymentType);
  }

  public double getFareAmount() {
    return fareAmount.getValue();
  }

  public void setFareAmount(double fareAmount) {
    this.fareAmount.setValue(fareAmount);
  }

  public double getSurcharge() {
    return surcharge.getValue();
  }

  public void setSurcharge(double surcharge) {
    this.surcharge.setValue(surcharge);
  }

  public double getMtaTax() {
    return mtaTax.getValue();
  }

  public void setMtaTax(double mtaTax) {
    this.mtaTax.setValue(mtaTax);
  }

  public double getTipAmount() {
    return tipAmount.getValue();
  }

  public void setTipAmount(double tipAmount) {
    this.tipAmount.setValue(tipAmount);
  }

  public double getTollsAmount() {
    return tollsAmount.getValue();
  }

  public void setTollsAmount(double tollsAmount) {
    this.tollsAmount.setValue(tollsAmount);
  }

  public double getTotalAmount() {
    return totalAmount.getValue();
  }

  public void setTotalAmount(double totalAmount) {
    this.totalAmount.setValue(totalAmount);
  }

  @Override
  public String toString() {
    return "["
        + String.join(
            ",",
            taxiIdentifier.toString(),
            hackLicense.toString(),
            pickupDatetime.toString(),
            tripTimeInSecs.toString(),
            tripDistance.toString(),
            pickupLongitude.toString(),
            pickupLatitude.toString(),
            dropoffLongitude.toString(),
            dropoffLatitude.toString(),
            paymentType.toString(),
            fareAmount.toString(),
            surcharge.toString(),
            mtaTax.toString(),
            tipAmount.toString(),
            tollsAmount.toString(),
            totalAmount.toString())
        + ']';
  }
}
