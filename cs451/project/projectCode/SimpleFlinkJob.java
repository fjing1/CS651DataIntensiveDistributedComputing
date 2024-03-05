package ca.uwaterloo.cs451.project;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment (Client)
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing (JobManager)
        env.enableCheckpointing(1000); // checkpoint every 1000ms
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // minimum time between checkpoints
        env.getCheckpointConfig().setCheckpointTimeout(10000); // checkpoint timeout
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // retain checkpoints on cancellation

        // Create a simple data stream (JobManager)
        DataStream<String> dataStream = env.fromElements("Hello", "World");

        // Transform the data stream (TaskManager)
        DataStream<String> transformedStream = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });

        // Print the result to stdout (JobManager)
        transformedStream.print();

        // Execute the job (Client)
        env.execute("Simple Flink Job");
    }
}
