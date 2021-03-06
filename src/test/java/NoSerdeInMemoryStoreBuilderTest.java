import static org.assertj.core.api.Assertions.assertThat;

import com.jsoniter.any.Any;
import java.util.Comparator;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NoSerdeInMemoryStoreBuilderTest {

  private static final String GLOBAL_STORE = "global.store";

  private static final String GLOBAL_STORE_TOPIC = "global.store.topic";

  private static final String GLOBAL_STORE_STRING_STRING = "global.store.string";

  private static final String GLOBAL_STORE_STRING_STRING_TOPIC = "global.store.string.topic";

  private Topology topology;

  private Serde<Any> jsonSerde;

  @BeforeEach
  private void setUp() {

    jsonSerde = Serdes.serdeFrom(new AnySerializer(), new AnyDeserializer());

    StreamsBuilder builder = new StreamsBuilder();

    builder.addGlobalStore(
        new NoSerdeInMemoryStoreBuilder<>(
            GLOBAL_STORE, Serdes.String(), jsonSerde, Comparator.naturalOrder()),
        GLOBAL_STORE_TOPIC,
        Consumed.with(Serdes.String(), jsonSerde),
        () -> new GlobalStoreUpdater<>(GLOBAL_STORE));

    builder.addGlobalStore(
        new NoSerdeInMemoryStoreBuilder<>(
            GLOBAL_STORE_STRING_STRING,
            Serdes.String(),
            Serdes.String(),
            Comparator.naturalOrder()),
        GLOBAL_STORE_STRING_STRING_TOPIC,
        Consumed.with(Serdes.String(), Serdes.String()),
        () -> new GlobalStoreUpdater<>(GLOBAL_STORE_STRING_STRING));

    topology = builder.build();
  }

  @Test
  public void globalStore() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, new Properties())) {
      TestInputTopic<String, String> globalStoreInputTopic =
          testDriver.createInputTopic(
              GLOBAL_STORE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());

      globalStoreInputTopic.pipeInput("KEY", "{\"hello\": \"world\"}");

      KeyValueStore<String, Any> globalStore =
          testDriver.<String, Any>getKeyValueStore(GLOBAL_STORE);

      assertThat(globalStore.get("KEY")).isNotNull();
      assertThat(globalStore.get("KEY").toString("hello")).isNotNull().isEqualTo("world");
    }
  }

  @Test
  public void overwrite() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, new Properties())) {
      TestInputTopic<String, String> globalStoreInputTopic =
          testDriver.createInputTopic(
              GLOBAL_STORE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());

      globalStoreInputTopic.pipeInput("KEY", "{}");
      globalStoreInputTopic.pipeInput("KEY", "{\"hello\": \"world\"}");

      KeyValueStore<String, Any> globalStore = testDriver.getKeyValueStore(GLOBAL_STORE);

      assertThat(globalStore.get("KEY")).isNotNull();
      assertThat(globalStore.get("KEY").toString("hello")).isNotNull().isEqualTo("world");
    }
  }

  @Test
  public void missingKey() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, new Properties())) {
      TestInputTopic<String, String> globalStoreInputTopic =
          testDriver.createInputTopic(
              GLOBAL_STORE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());

      globalStoreInputTopic.pipeInput("KEY", "{\"hello\": \"world\"}");

      KeyValueStore<String, Any> globalStore = testDriver.getKeyValueStore(GLOBAL_STORE);

      assertThat(globalStore.get("MISSING_KEY")).isNull();
    }
  }

  @Test
  public void removeKey() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, new Properties())) {
      TestInputTopic<String, String> globalStoreInputTopic =
          testDriver.createInputTopic(
              GLOBAL_STORE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());

      globalStoreInputTopic.pipeInput("KEY", "{\"hello\": \"world\"}");
      globalStoreInputTopic.pipeInput("KEY", (String) null);

      KeyValueStore<String, Any> globalStore = testDriver.getKeyValueStore(GLOBAL_STORE);

      assertThat(globalStore.get("KEY")).isNull();
    }
  }

  @Test
  public void range() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, new Properties())) {
      TestInputTopic<String, String> globalStoreInputTopic =
          testDriver.createInputTopic(
              GLOBAL_STORE_STRING_STRING_TOPIC,
              Serdes.String().serializer(),
              Serdes.String().serializer());

      globalStoreInputTopic.pipeInput("A", "A");
      globalStoreInputTopic.pipeInput("KEY_1", "VALUE_1");
      globalStoreInputTopic.pipeInput("KEY_2", "VALUE_2");
      globalStoreInputTopic.pipeInput("KEY_3", "VALUE_3");
      globalStoreInputTopic.pipeInput("NO_KEY", "NO_KEY");

      KeyValueStore<String, String> globalStore =
          testDriver.getKeyValueStore(GLOBAL_STORE_STRING_STRING);

      assertThat(globalStore.range("KEY", "KEY_99")).isNotNull();
      assertThat(globalStore.range("KEY", "KEY_99")).isNotEmpty();
      assertThat(globalStore.range("KEY", "KEY_99"))
          .containsExactly(
              KeyValue.pair("KEY_1", "VALUE_1"),
              KeyValue.pair("KEY_2", "VALUE_2"),
              KeyValue.pair("KEY_3", "VALUE_3"));
    }
  }
}
