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

public class AveragePlot extends DoFn<Iterable<TimestampedValue<Double>>, byte[]> {

  @ProcessElement
  public void processElement(
      @Element Iterable<TimestampedValue<Double>> averages, OutputReceiver<byte[]> out)
      throws IOException {
    List<TimestampedValue<Double>> sorted =
        StreamSupport.stream(averages.spliterator(), true)
            .sorted(Comparator.comparing(TimestampedValue::getTimestamp))
            .collect(Collectors.toList());
    XYChart chart =
        new XYChartBuilder()
            .width(800)
            .height(600)
            .title(getClass().getSimpleName())
            .xAxisTitle("Time")
            .yAxisTitle("Average")
            .build();

    List<Double> sortedaverages =
        sorted.stream().map(TimestampedValue::getValue).collect(Collectors.toList());
    List<Date> sortedTimestamps =
        sorted.stream().map(v -> v.getTimestamp().toDate()).collect(Collectors.toList());

    chart.addSeries("Distinct Counts", sortedTimestamps, sortedaverages);

    byte[] chartBytes = BitmapEncoder.getBitmapBytes(chart, BitmapEncoder.BitmapFormat.PNG);
    out.output(chartBytes);
  }
}
