import static org.assertj.core.api.Assertions.assertThat;

import com.jsoniter.any.Any;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

  private Topology topology;

  private Serde<Any> jsonSerde;

  @BeforeEach
  private void setUp() {

    jsonSerde = Serdes.serdeFrom(new AnySerializer(), new AnyDeserializer());

    StreamsBuilder builder = new StreamsBuilder();

    builder.addGlobalStore(
        new NoSerdeInMemoryStoreBuilder<>(GLOBAL_STORE, Serdes.String(), jsonSerde),
        GLOBAL_STORE_TOPIC,
        Consumed.with(Serdes.String(), jsonSerde),
        () -> new GlobalStoreUpdater<>(GLOBAL_STORE));

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
}
