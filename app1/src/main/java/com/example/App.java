// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;

public class App {

    public interface Options extends StreamingOptions {
		@Description("Input text to print.")
		@Default.String("My input text")
		String getInputText();

		void setInputText(String value);
	}

	public static void main(String[] args) {
		var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(4);

		var pipeline = Pipeline.create(options);

        // PCollection<Integer> numbers = pipeline.apply(CreateSequence.from(1).to(10));
        PCollection<Long> numbers = pipeline.apply(GenerateSequence.from(1).to(10));

        // Filter out even numbers
        PCollection<Long> filteredNumbers = numbers.apply(Filter.by(number -> number % 2 != 0));

        PCollection<Long> filteredNumbersLT = filteredNumbers.apply(Filter.by(n -> n > 5));

        // Convert integers to strings for writing to output
        PCollection<String> output = filteredNumbersLT.apply(MapElements.into(TypeDescriptors.strings()).via(Object::toString));

        // Write output to a text file
        // output.apply(TextIO.write().to("output.txt").withoutSharding());

        output.apply(ParDo.of(new PrintFn()));

        // Run the Pipeline
        pipeline.run();
    }

    static class PrintFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String element) {
            System.out.println(element);
        }
    }
}
