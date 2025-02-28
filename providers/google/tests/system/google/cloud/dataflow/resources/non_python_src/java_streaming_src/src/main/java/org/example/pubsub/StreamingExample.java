package org.example.pubsub;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import java.util.logging.Logger;


public class StreamingExample {

    public interface StreamingExampleOptions extends PipelineOptions, StreamingOptions {
        @Description("Input Pub/Sub Topic")
        @Validation.Required
        String getInput_topic();
        void setInput_topic(String inputTopic);

        @Description("Output Pub/Sub Topic")
        @Validation.Required
        String getOutput_topic();
        void setOutput_topic(String outputTopic);
    }

    public static void main(String[] args) {
        StreamingExampleOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(StreamingExampleOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getInput_topic()))
            .apply("WriteToPubSub", PubsubIO.writeStrings().to(options.getOutput_topic()));

        pipeline.run();
    }
}
