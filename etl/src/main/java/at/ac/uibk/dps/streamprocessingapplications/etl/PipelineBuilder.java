package at.ac.uibk.dps.streamprocessingapplications.etl;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.AnnotationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.InterpolationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.FitSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.GridSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.StoreStringInDBSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.WriteStringSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PipelineBuilder {
    static Pipeline buildTAXIPipeline(FlinkPipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> etl_strings =
                pipeline
                        .apply(new ReadSenMLSource("senml-source"))
                        .apply(
                                new ETLPipeline<>(
                                        TypeDescriptor.of(TaxiRide.class),
                                        TaxiSenMLParserJSON::parseSenMLPack,
                                        new RangeFilterFunction(),
                                        // TaxiTestObjects.buildTestBloomFilter(),
                                        null,
                                        new InterpolationFunction(),
                                        5,
                                        new AnnotationFunction()))
                        .apply(
                                "Serialize SenML to String",
                                MapElements.into(TypeDescriptors.strings()).via(TaxiRide::toString));
        etl_strings.apply(new WriteStringSink("senml-cleaned"));
        etl_strings.apply(new StoreStringInDBSink("senml-cleaned"));

        return pipeline;
    }

    static Pipeline buildFITPipeline(FlinkPipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> etl_strings =
                pipeline
                        .apply(new ReadSenMLSource("senml-source"))
                        .apply(
                                new ETLPipeline<>(
                                        TypeDescriptor.of(FitnessMeasurements.class),
                                        FitSenMLParserJSON::parseSenMLPack,
                                        new at.ac.uibk.dps.streamprocessingapplications.etl.fit.RangeFilterFunction(),
                                        // TaxiTestObjects.buildTestBloomFilter(),
                                        null,
                                        new at.ac.uibk.dps.streamprocessingapplications.etl.fit.InterpolationFunction(),
                                        5,
                                        new at.ac.uibk.dps.streamprocessingapplications.etl.fit.AnnotationFunction()))
                        .apply(
                                "Serialize SenML to String",
                                MapElements.into(TypeDescriptors.strings()).via(FitnessMeasurements::toString));
        etl_strings.apply(new WriteStringSink("senml-cleaned"));
        etl_strings.apply(new StoreStringInDBSink("senml-cleaned"));

        return pipeline;
    }

    static Pipeline buildGRIDPipeline(FlinkPipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> etl_strings =
                pipeline
                        .apply(new ReadSenMLSource("senml-source"))
                        .apply(
                                new ETLPipeline<>(
                                        TypeDescriptor.of(GridMeasurement.class),
                                        GridSenMLParserJSON::parseSenMLPack,
                                        new at.ac.uibk.dps.streamprocessingapplications.etl.grid.RangeFilterFunction(),
                                        // TaxiTestObjects.buildTestBloomFilter(),
                                        null,
                                        new at.ac.uibk.dps.streamprocessingapplications.etl.grid.InterpolationFunction(),
                                        5,
                                        new at.ac.uibk.dps.streamprocessingapplications.etl.grid.AnnotationFunction()))
                        .apply(
                                "Serialize SenML to String",
                                MapElements.into(TypeDescriptors.strings()).via(GridMeasurement::toString));
        etl_strings.apply(new WriteStringSink("senml-cleaned"));
        etl_strings.apply(new StoreStringInDBSink("senml-cleaned"));

        return pipeline;
    }
}
