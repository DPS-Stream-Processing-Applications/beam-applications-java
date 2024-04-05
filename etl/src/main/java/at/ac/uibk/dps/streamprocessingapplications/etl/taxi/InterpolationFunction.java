package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InterpolationFunction implements Function<Iterable<TaxiRide>, Iterable<TaxiRide>> {
  private double meanTripTime;
  private double meanTripDistance;
  private double meanFareAmount;
  private double meanTollsAmount;
  private double meanTotalAmount;

  InterpolationFunction() {
    this.meanTripTime = 0;
    this.meanTripDistance = 0;
    this.meanFareAmount = 0;
    this.meanTollsAmount = 0;
    this.meanTotalAmount = 0;
  }

  @Override
  public Iterable<TaxiRide> apply(Iterable<TaxiRide> taxiRides) {
    this.calculateMeans(taxiRides);
    return StreamSupport.stream(taxiRides.spliterator(), false)
        .map(this::interpolate)
        .collect(Collectors.toList());
  }

  private TaxiRide interpolate(TaxiRide taxiRide) {
    if (taxiRide.getTripTimeInSecs().isEmpty()) {
      taxiRide.setTripTimeInSecs(this.meanTripTime);
    }
    if (taxiRide.getTripDistance().isEmpty()) {
      taxiRide.setTripDistance(this.meanTripDistance);
    }
    if (taxiRide.getFareAmount().isEmpty()) {
      taxiRide.setFareAmount(this.meanFareAmount);
    }
    if (taxiRide.getTollsAmount().isEmpty()) {
      taxiRide.setTollsAmount(this.meanTollsAmount);
    }
    if (taxiRide.getTotalAmount().isEmpty()) {
      taxiRide.setTotalAmount(this.meanTotalAmount);
    }

    return taxiRide;
  }

  private void calculateMeans(Iterable<TaxiRide> taxiRides) {
    Collection<Double> validTripTimes = new ArrayList<>();
    Collection<Double> validTripDistances = new ArrayList<>();
    Collection<Double> validFareAmounts = new ArrayList<>();
    Collection<Double> validTollsAmounts = new ArrayList<>();
    Collection<Double> validTotalAmounts = new ArrayList<>();

    for (TaxiRide taxiRide : taxiRides) {
      taxiRide.getTripTimeInSecs().ifPresent(validTripTimes::add);
      taxiRide.getTripDistance().ifPresent(validTripDistances::add);
      taxiRide.getFareAmount().ifPresent(validFareAmounts::add);
      taxiRide.getTollsAmount().ifPresent(validTollsAmounts::add);
      taxiRide.getTotalAmount().ifPresent(validTotalAmounts::add);
    }

    this.meanTripTime = validTripTimes.stream().mapToDouble(d -> d).average().orElse(0.0);
    this.meanTripDistance = validTripDistances.stream().mapToDouble(d -> d).average().orElse(0.0);
    this.meanFareAmount = validFareAmounts.stream().mapToDouble(d -> d).average().orElse(0.0);
    this.meanTollsAmount = validTollsAmounts.stream().mapToDouble(d -> d).average().orElse(0.0);
    this.meanTotalAmount = validTotalAmounts.stream().mapToDouble(d -> d).average().orElse(0.0);
  }

  @Override
  public <V> Function<V, Iterable<TaxiRide>> compose(
      Function<? super V, ? extends Iterable<TaxiRide>> before) {
    return Function.super.compose(before);
  }

  @Override
  public <V> Function<Iterable<TaxiRide>, V> andThen(
      Function<? super Iterable<TaxiRide>, ? extends V> after) {
    return Function.super.andThen(after);
  }
}
