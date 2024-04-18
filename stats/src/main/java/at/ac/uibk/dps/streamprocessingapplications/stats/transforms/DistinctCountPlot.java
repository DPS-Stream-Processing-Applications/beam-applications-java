package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TimestampedValue;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

public class DistinctCountPlot extends DoFn<Iterable<TimestampedValue<Long>>, byte[]> {

  @ProcessElement
  public void processElement(
      @Element Iterable<TimestampedValue<Long>> counts, OutputReceiver<byte[]> out)
      throws IOException {
    List<TimestampedValue<Long>> sorted =
        StreamSupport.stream(counts.spliterator(), true)
            .sorted(Comparator.comparing(TimestampedValue::getTimestamp))
            .collect(Collectors.toList());
    XYChart chart =
        new XYChartBuilder()
            .width(800)
            .height(600)
            .title(getClass().getSimpleName())
            .xAxisTitle("Time")
            .yAxisTitle("Distinct Count")
            .build();

    List<Long> sortedCounts =
        sorted.stream().map(TimestampedValue::getValue).collect(Collectors.toList());
    List<Date> sortedTimestamps =
        sorted.stream().map(v -> v.getTimestamp().toDate()).collect(Collectors.toList());

    chart.addSeries("Distinct Counts", sortedTimestamps, sortedCounts);

    byte[] chartBytes = BitmapEncoder.getBitmapBytes(chart, BitmapEncoder.BitmapFormat.PNG);
    out.output(chartBytes);
  }
}
