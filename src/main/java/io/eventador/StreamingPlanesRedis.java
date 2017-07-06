package io.eventador;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

// Redis
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Collector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StreamingPlanesRedis {
    static String REDIS_HOST = "127.0.0.1";
    static Integer REDIS_PORT = 6379;

	public static void main(String[] args) throws Exception {
		/* get properties file, eg. /tmp/planes.properties

            topic: defaultsink
            bootstrap.servers: f0ac128f-kafka0.va.eventador.io:9092
            auto.offset.reset: earliest
            plane-rollups-topic: planes-out
        */
		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

		// create streaming environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// enable event time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// enable fault-tolerance, 5s checkpointing
		env.enableCheckpointing(5000);

		// enable restarts
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, 500L));
		env.setStateBackend(new FsStateBackend("file:///tmp/state-backend"));

		// run each operator separately
		env.disableOperatorChaining();

		// Stream data from Kafka
		Properties kParams = params.getProperties();
		kParams.setProperty("group.id", UUID.randomUUID().toString());
		DataStream<ObjectNode> inputStream = env.addSource(new FlinkKafkaConsumer010<>(params.getRequired("topic"), new JSONDeserializationSchema(), kParams)).name("Kafka 0.10 Source")
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.minutes(1L)) {
				@Override
				public long extractTimestamp(ObjectNode jsonNodes) {
					return jsonNodes.get("timestamp").asLong();
				}
			}).name("Timestamp extractor");

		// filter out invalid records, eg. grep -v
		DataStream<ObjectNode> planesView = inputStream.filter(jsonNode -> jsonNode.has("altitude")).name("Filter valid plane records");

		// write to planes log
		RollingSink<ObjectNode> rollingSink = new RollingSink<>(params.get("sinkPath", "/tmp/rolling-plans"));
		rollingSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd-HH-mm")); // do a bucket for each minute
		planesView.addSink(rollingSink).name("Rolling Planes FileSystem Sink");

		// build aggregates (count per ICAO) using window (10 seconds tumbling):
		DataStream<Tuple3<Long, String, Long>> planeCounts = planesView.keyBy(jsonNode -> jsonNode.get("icao").asText())
			.timeWindow(Time.seconds(10))
			.apply(new Tuple3<>(0L, "", 0L), new JsonFoldCounter(), new CountEmitter()).name("Count per ICAO (10 seconds tumbling)")
            .keyBy(1);

        // create a datastream consisting of only a tuple w/ ICAO and then the whole document
        // for writing raw planes to redis
        DataStream<Tuple2<String, String>> redisStream = planesView.map(new MapFunction<ObjectNode, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(ObjectNode node) throws Exception {
                // String icao = node.get("icao").asText();
                Tuple2<String, String> t = new Tuple2<>(node.get("icao").asText(), node.toString());
                return t;
            }
        }).name("Redis Stream");

        // Print plane stream for debugging
        planeCounts.print();

        // Write plane agg stream back to Kafka
		planeCounts.addSink(new FlinkKafkaProducer010<>(params.getRequired("plane-rollups-topic"), new ErikSerSchema(), params.getProperties())).name("Write topN to Kafka");

        // Write to Redis
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
            .setHost(REDIS_HOST)
            .setPort(REDIS_PORT)
            .build();
		redisStream.addSink(new RedisSink<Tuple2<String, String>>(conf, new ErikRedisMapper())).name("Redis Sink");

		env.execute("Streaming Planes");

	}

	private static class JsonFoldCounter implements FoldFunction<ObjectNode, Tuple3<Long, String, Long>> {
		@Override
		public Tuple3<Long, String, Long> fold(Tuple3<Long, String, Long> current, ObjectNode o) throws Exception {
			current.f0++;
			return current;
		}
	}

	private static class CountEmitter implements WindowFunction<Tuple3<Long, String, Long>, Tuple3<Long, String, Long>, String, TimeWindow> {
		@Override
		public void apply(String key, TimeWindow timeWindow, Iterable<Tuple3<Long, String, Long>> iterable, Collector<Tuple3<Long, String, Long>> collector) throws Exception {
			long count = iterable.iterator().next().f0;
			collector.collect(Tuple3.of(count, key, timeWindow.getStart()));
		}
	}

	private static class ErikSerSchema implements SerializationSchema<Tuple3<Long, String, Long>> {

		@Override
		public byte[] serialize(Tuple3<Long, String, Long> tuple3) {
			return (tuple3.f0.toString() + " - " + tuple3.toString()).getBytes();
		}
	}

    // hacky Redis mapper
	private static class ErikRedisMapper implements RedisMapper<Tuple2<String, String>>{

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.SET, "SET");
		}

		@Override
		public String getKeyFromData(Tuple2<String, String> data) {
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple2<String, String> data) {
			return data.f1;
		}
	}

}
