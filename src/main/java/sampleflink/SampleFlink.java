package sampleflink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SampleFlink {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String outputBasePath = "s3a://shenavaa-public/output/";



		DataStream<String> dataStream = env.fromElements(
				"rec1",
				"rec2");

		final FileSink<String> sink = FileSink
    			.forRowFormat(new Path(outputBasePath), new SimpleStringEncoder<String>("UTF-8"))
    			.withRollingPolicy(
        			DefaultRollingPolicy.builder()
            			.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            			.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            			.withMaxPartSize(1024 * 1024 * 1024)
            			.build())
			.build();
		dataStream.sinkTo(sink);		
		env.execute();
	}

}

