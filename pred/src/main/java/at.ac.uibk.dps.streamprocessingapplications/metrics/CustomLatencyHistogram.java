package at.ac.uibk.dps.streamprocessingapplications.metrics;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

public class CustomLatencyHistogram implements Histogram {

  private final List<Long> latencies;

  public CustomLatencyHistogram() {
    this.latencies = new ArrayList<>();
  }

  @Override
  public void update(long value) {
    latencies.add(value);
  }

  @Override
  public long getCount() {
    return latencies.size();
  }

  @Override
  public HistogramStatistics getStatistics() {
    return new CustomHistogramStatistics(latencies);
  }

  private static class CustomHistogramStatistics extends HistogramStatistics {

    private final List<Long> latencies;

    public CustomHistogramStatistics(List<Long> latencies) {
      this.latencies = new ArrayList<>(latencies);
    }

    @Override
    public double getMean() {
      if (latencies.isEmpty()) return 0;
      long sum = 0;
      for (long latency : latencies) {
        sum += latency;
      }
      return (double) sum / latencies.size();
    }

    @Override
    public double getStdDev() {
      return 0;
    }

    @Override
    public long getMin() {
      return latencies.stream().min(Long::compareTo).orElse(0L);
    }

    @Override
    public long getMax() {
      return latencies.stream().max(Long::compareTo).orElse(0L);
    }

    @Override
    public double getQuantile(double quantile) {
      if (latencies.isEmpty()) return 0;
      List<Long> sortedLatencies = new ArrayList<>(latencies);
      sortedLatencies.sort(Long::compareTo);
      int index = (int) Math.ceil(quantile * sortedLatencies.size()) - 1;
      return sortedLatencies.get(Math.max(index, 0));
    }

    @Override
    public long[] getValues() {
      long[] values = new long[latencies.size()];
      for (int i = 0; i < latencies.size(); i++) {
        values[i] = latencies.get(i);
      }
      return values;
    }

    @Override
    public int size() {
      return latencies.size();
    }
  }
}
