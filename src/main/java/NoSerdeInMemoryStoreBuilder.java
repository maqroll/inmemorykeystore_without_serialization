import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

public class NoSerdeInMemoryStoreBuilder<K extends Comparable, V>
    implements StoreBuilder<KeyValueStore<K, V>> {

  private final String name;

  private final Serde<K> serdeKey;

  private final Serde<V> serdeValue;

  private final Comparator<K> keyComparator;

  public NoSerdeInMemoryStoreBuilder(String name, Serde<K> serdeKey, Serde<V> serdeValue) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(serdeKey);
    Objects.requireNonNull(serdeValue);
    this.name = name;
    this.serdeKey = serdeKey;
    this.serdeValue = serdeValue;
    this.keyComparator = Comparator.naturalOrder();
  }

  public NoSerdeInMemoryStoreBuilder(
      String name, Serde<K> serdeKey, Serde<V> serdeValue, Comparator<K> keyComparator) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(serdeKey);
    Objects.requireNonNull(serdeValue);
    this.name = name;
    this.serdeKey = serdeKey;
    this.serdeValue = serdeValue;
    this.keyComparator = keyComparator;
  }

  @Override
  public StoreBuilder<KeyValueStore<K, V>> withCachingEnabled() {
    return this;
  }

  @Override
  public StoreBuilder<KeyValueStore<K, V>> withCachingDisabled() {
    return this;
  }

  @Override
  public StoreBuilder<KeyValueStore<K, V>> withLoggingEnabled(Map<String, String> config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StoreBuilder<KeyValueStore<K, V>> withLoggingDisabled() {
    return this;
  }

  @Override
  public KeyValueStore<K, V> build() {
    return new NoSerdeInMemoryKeyValueStore<K, V>(name, serdeKey, serdeValue, keyComparator);
  }

  @Override
  public Map<String, String> logConfig() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean loggingEnabled() {
    return false;
  }

  @Override
  public String name() {
    return name;
  }
}
