import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.example.PubSubSubscriptionToText;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;




public class PubSubSubscriptionToTextTest implements Serializable {

    private static final String[] PUBSUB_MSGS_ARRAY = new String[] {
            "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"CREATE\",\"data\":{\"nome\":\"Arthur Parrado\",\"cargo\":\"Programador\"}}",
            "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"UPDATE\",\"data\":{\"id\":123,\"nome\":\"Artur Schutz\",\"cargo\":\"Diretor\"}}",
            "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"DELETE\",\"data\":{\"id\":123}}",
            "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"DELETE\",\"data\"{\"id\":123}}",
            "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"data\":{\"id\":123}}",
            "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"BATMAN\",\"data\":{\"id\":123}}",
            "{\"timestamp\":\"2019-10-03Z01:02:03.123456\",\"type\":\"UPDATE\",\"data\":{\"id\":123}}"
    };

    private static final List<String> PUBSUB_MSGS = Arrays.asList(PUBSUB_MSGS_ARRAY);

    @Rule
    public final transient TestPipeline pipeline =
            TestPipeline.fromOptions(getTestPipelineOptions());

    private PipelineOptions getTestPipelineOptions() {
        final PipelineOptions options = PipelineOptionsFactory.create().as(PipelineOptions.class);
        options.setTempLocation("/temp/");
        return options;
    }

    @Test
    public void testValidateEventJsonSchema() {
        Pipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

        PCollection<String> pubsubMessages = p.apply(Create.of(PUBSUB_MSGS)).setCoder(StringUtf8Coder.of());

        PCollection<KV<String, String>> jsonMessages =
                pubsubMessages
                        .apply("ValidateJsonEvents",
                                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                        .via(PubSubSubscriptionToText::validateEventJsonSchema));

        PCollection<String> validMessages = jsonMessages
                .apply("FilterByValidEvents",
                        Filter.by((KV<String, String> input) ->
                        PubSubSubscriptionToText.getEventsByValidationStatus(input, "VALID_MESSAGE")))
                .apply("GetJsonValidEvents",
                        MapElements.into(TypeDescriptors.strings()).via(KV::getValue));

        PAssert.that(validMessages)
                .containsInAnyOrder(
                        "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"CREATE\",\"data\":{\"nome\":\"Arthur Parrado\",\"cargo\":\"Programador\"}}",
                        "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"UPDATE\",\"data\":{\"id\":123,\"nome\":\"Artur Schutz\",\"cargo\":\"Diretor\"}}",
                        "{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"DELETE\",\"data\":{\"id\":123}}"
                );

        PCollection<String> invalidMessages = jsonMessages
                .apply("FilterByInvalidEvents",
                        Filter.by((KV<String, String> input) ->
                        PubSubSubscriptionToText.getEventsByValidationStatus(input, "INVALID_MESSAGE")))
                .apply("GetJsonInvalidEvents",
                        MapElements.into(TypeDescriptors.strings()).via(KV::getValue));

        PAssert.that(invalidMessages)
                .containsInAnyOrder(
                        "{\"error\":\"Unexpected character ('{' (code 123)): was expecting a colon to separate field name and value\n at [Source: (String)\"{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"DELETE\",\"data\"{\"id\":123}}\"; line: 1, column: 66]\",\"message\":\"{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"DELETE\",\"data\"{\"id\":123}}\"}",
                        "{\"error\":\"INVALID_SCHEMA\",\"message\":\"{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"data\":{\"id\":123}}\"}",
                        "{\"error\":\"INVALID_TYPE\",\"message\":\"{\"timestamp\":\"2019-10-03T01:02:03.123456\",\"type\":\"BATMAN\",\"data\":{\"id\":123}}\"}",
                        "{\"error\":\"Unparseable date: \"2019-10-03Z01:02:03.123456\"\",\"message\":\"{\"timestamp\":\"2019-10-03Z01:02:03.123456\",\"type\":\"UPDATE\",\"data\":{\"id\":123}}\"}"
                );

        p.run();


    }



}
