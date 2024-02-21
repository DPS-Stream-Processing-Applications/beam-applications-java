package at.ac.uibk.dps.streamprocessingapplications;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class EtlJob {

    public static void main(String[] args) {
        var options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(4);

        var pipeline = Pipeline.create(options);

        PCollection<Long> numbers = pipeline.apply(GenerateSequence.from(1).to(10));

        PCollection<Long> filteredNumbers = numbers.apply(Filter.by(number -> number % 2 != 0));

        PCollection<Long> filteredNumbersLT = filteredNumbers.apply(Filter.by(n -> n > 5));

        PCollection<String> output =
                filteredNumbersLT.apply(
                        MapElements.into(TypeDescriptors.strings()).via(Object::toString));

        output.apply(ParDo.of(new PrintFn()));

        pipeline.run();
    }

    static class PrintFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String element) {
            System.out.println(element);
        }
    }
}
