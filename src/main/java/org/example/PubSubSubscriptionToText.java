package org.example;

import avro.shaded.com.google.common.collect.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class PubSubSubscriptionToText {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriptionToText.class);

    /**
     * Options supported by the pipeline.
     *
     * <p>Inherits standard and streaming configuration options.</p>
     */
    public interface PubSubSubscriptionToTextOptions extends PipelineOptions, StreamingOptions {

        @Description("The Cloud Pub/Sub subscription to read from.")
        @Validation.Required
        ValueProvider<String> getInputSubscription();
        void setInputSubscription(ValueProvider<String> value);

        @Description("The directory to output files to. Must end with a slash.")
        @Validation.Required
        ValueProvider<String> getOutputDirectory();
        void setOutputDirectory(ValueProvider<String> value);

        @Description("The directory to output error message files to. Must end with a slash.")
        @Validation.Required
        ValueProvider<String> getOutputErrorDirectory();
        void setOutputErrorDirectory(ValueProvider<String> value);

        @Description("The directory to output temp files to.")
        @Validation.Required
        ValueProvider<String> getOutputTmpDirectory();
        void setOutputTmpDirectory(ValueProvider<String> value);

        @Description("The window duration in which data will be written in minutes. ")
        @Default.Long(15)
        long getWindowDuration();
        void setWindowDuration(long value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(1)
        Integer getNumShards();
        void setNumShards(Integer value);

    }

    public static KV<String, String> validateEventJsonSchema(String input){
            List<String> SCHEMA = Arrays.asList("timestamp", "type", "data");
            List<String> EVENT_TYPE = Arrays.asList("CREATE", "UPDATE", "DELETE");

            String errorFormat = "{\"error\":\"%s\",\"message\":\"%s\"}";

            try {
                JsonNode rootNode = new ObjectMapper().readTree(input);
                List<String> mkeys = Lists.newArrayList(rootNode.fieldNames());
                if (!mkeys.containsAll(SCHEMA)) {
                    throw new Exception("INVALID_SCHEMA");
                }
                String timestamp = rootNode.get("timestamp").toString().replace("\"", "");
                SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                format.parse(timestamp.replace("T", " "));
                if (!EVENT_TYPE.contains(rootNode.get("type").toString().replace("\"", ""))) {
                    throw new Exception("INVALID_TYPE");
                }
                return KV.of("VALID_MESSAGE", input);
            } catch (Exception e) {
                return KV.of("INVALID_MESSAGE", String.format(errorFormat, e.getMessage(), input));
            }
    };

    public static Boolean getEventsByValidationStatus(KV<String, String> input, String status){
        if(input.getKey().equals(status)){
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    };


    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return  The result of the pipeline execution.
     */

    public static PipelineResult run(PubSubSubscriptionToTextOptions options) {
        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        /*
         * Steps:
         *   1) Read string messages from PubSub
         *   2) Window the messages into minute intervals specified by the executor.
         *   3) Check if the message is a json
         *   4) Check if the message has a valid schema
         *   3) Output the windowed files to GCS
         */

        PCollection<String> pubsubMessages =
                p.apply("ReadPubSubEvents",
                        PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
                .apply("WindowingEvents",
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowDuration()))));


        PCollection<KV<String, String>> jsonMessages =
                pubsubMessages
                        .apply("ValidateJsonEvents",
                                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                        .via(PubSubSubscriptionToText::validateEventJsonSchema));


        jsonMessages
                .apply("FilterByValidEvents",
                        Filter.by((KV<String, String> input) -> getEventsByValidationStatus(input, "VALID_MESSAGE")))
                .apply("GetJsonValidEvents",
                        MapElements.into(TypeDescriptors.strings()).via(KV::getValue))
                .apply("WriteFiles",
                        TextIO.write().withWindowedWrites()
                                .withNumShards(options.getNumShards())
                                .to(options.getOutputDirectory())
                                .withTempDirectory(ValueProvider.NestedValueProvider.of(
                                        options.getOutputTmpDirectory(),
                                        (SerializableFunction<String, ResourceId>) FileBasedSink::convertToFileResourceIfPossible)));

        jsonMessages
                .apply("FilterByInvalidEvents",
                        Filter.by((KV<String, String> input) -> getEventsByValidationStatus(input, "INVALID_MESSAGE")))
                .apply("GetJsonInvalidEvents",
                        MapElements.into(TypeDescriptors.strings()).via(KV::getValue))
                .apply("WriteErrorFiles",
                TextIO.write().withWindowedWrites()
                        .withNumShards(options.getNumShards())
                        .to(options.getOutputErrorDirectory())
                        .withTempDirectory(ValueProvider.NestedValueProvider.of(
                                options.getOutputTmpDirectory(),
                                (SerializableFunction<String, ResourceId>) FileBasedSink::convertToFileResourceIfPossible)));

        // Execute the pipeline and return the result.
        return p.run();

    }

    /**
     * Main entry point for executing the pipeline.
     * @param args  The command-line arguments to the pipeline.
     */

    public static void main(String[] args) {

        PipelineOptionsFactory.register(PubSubSubscriptionToTextOptions.class);
        PubSubSubscriptionToTextOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubSubscriptionToTextOptions.class);

        options.setStreaming(true);

        run(options);

    }

}
