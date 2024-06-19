package org.apache.flink.statefun.playground.java.greeter.types.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class Taxi_Trip extends TableServiceEntity {

    private String trip_time_in_secs, trip_distance;
    private String pickup_longitude,
            pickup_latitude,
            dropoff_longitude,
            dropoff_latitude,
            payment_type;
    private String fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount;
    private String taxi_identifier, hack_license, pickup_datetime, drop_datetime;
    private long rangeTs;

    public long getRangeTs() {
        return rangeTs;
    }

    public void setRangeTs(long rangeTs) {
        this.rangeTs = rangeTs;
    }

    public String getDrop_datetime() {
        return drop_datetime;
    }

    public void setDrop_datetime(String drop_datetime) {
        this.drop_datetime = drop_datetime;
    }

    public String getTaxi_identifier() {
        return taxi_identifier;
    }

    public void setTaxi_identifier(String taxi_identifier) {
        this.taxi_identifier = taxi_identifier;
    }

    public String getHack_license() {
        return hack_license;
    }

    public void setHack_license(String hack_license) {
        this.hack_license = hack_license;
    }

    public String getPickup_datetime() {
        return pickup_datetime;
    }

    public void setPickup_datetime(String l) {
        this.pickup_datetime = l;
    }

    public String getTrip_time_in_secs() {
        return trip_time_in_secs;
    }

    public void setTrip_time_in_secs(String trip_time_in_secs) {
        this.trip_time_in_secs = trip_time_in_secs;
    }

    public String getTrip_distance() {
        return trip_distance;
    }

    public void setTrip_distance(String trip_distance) {
        this.trip_distance = trip_distance;
    }

    public String getPickup_longitude() {
        return pickup_longitude;
    }

    public void setPickup_longitude(String pickup_longitude) {
        this.pickup_longitude = pickup_longitude;
    }

    public String getPickup_latitude() {
        return pickup_latitude;
    }

    public void setPickup_latitude(String pickup_latitude) {
        this.pickup_latitude = pickup_latitude;
    }

    public String getDropoff_longitude() {
        return dropoff_longitude;
    }

    public void setDropoff_longitude(String dropoff_longitude) {
        this.dropoff_longitude = dropoff_longitude;
    }

    public String getDropoff_latitude() {
        return dropoff_latitude;
    }

    public void setDropoff_latitude(String dropoff_latitude) {
        this.dropoff_latitude = dropoff_latitude;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    public String getFare_amount() {
        return fare_amount;
    }

    public void setFare_amount(String fare_amount) {
        this.fare_amount = fare_amount;
    }

    public String getSurcharge() {
        return surcharge;
    }

    public void setSurcharge(String surcharge) {
        this.surcharge = surcharge;
    }

    public String getMta_tax() {
        return mta_tax;
    }

    public void setMta_tax(String mta_tax) {
        this.mta_tax = mta_tax;
    }

    public String getTip_amount() {
        return tip_amount;
    }

    public void setTip_amount(String tip_amount) {
        this.tip_amount = tip_amount;
    }

    public String getTolls_amount() {
        return tolls_amount;
    }

    public void setTolls_amount(String tolls_amount) {
        this.tolls_amount = tolls_amount;
    }

    public String getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(String total_amount) {
        this.total_amount = total_amount;
    }
}
