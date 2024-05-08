package at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Event implements Delayed {
  private final String eventString;
  private final Long arrivalTime;

  public Event(String eventString, long arrivalTime) {
    this.eventString = eventString;
    this.arrivalTime = arrivalTime;
  }

  public String getEventString() {
    return eventString;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    long diff = this.arrivalTime - System.currentTimeMillis();
    return unit.convert(diff, TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    return this.arrivalTime.compareTo(((Event) o).arrivalTime);
  }
}
