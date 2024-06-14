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

/* NOTE:
 * Due to the ambiguity of the plotting from the original implementation,
 * I decided to plot the `mean` of the predicted values within the prediction horizon.
 */
// `
public class KalmanRegressionPlot extends DoFn<Iterable<TimestampedValue<List<Double>>>, byte[]> {

  @ProcessElement
  public void processElement(
      @Element Iterable<TimestampedValue<List<Double>>> predictedValues, OutputReceiver<byte[]> out)
      throws IOException {
    List<TimestampedValue<List<Double>>> sorted =
        StreamSupport.stream(predictedValues.spliterator(), true)
            .sorted(Comparator.comparing(TimestampedValue::getTimestamp))
            .collect(Collectors.toList());
    XYChart chart =
        new XYChartBuilder()
            .width(800)
            .height(600)
            .title(getClass().getSimpleName())
            .xAxisTitle("Time")
            .yAxisTitle("Kalman")
            .build();

    List<Double> sortedMeans =
        sorted.stream()
            .map(TimestampedValue::getValue)
            .map(list -> list.stream().mapToDouble(Double::doubleValue).average().orElse(0.0))
            .collect(Collectors.toList());
    List<Date> sortedTimestamps =
        sorted.stream().map(v -> v.getTimestamp().toDate()).collect(Collectors.toList());

    chart.addSeries("Mean Predictions", sortedTimestamps, sortedMeans);

    byte[] chartBytes = BitmapEncoder.getBitmapBytes(chart, BitmapEncoder.BitmapFormat.PNG);
    out.output(chartBytes);
  }
}
