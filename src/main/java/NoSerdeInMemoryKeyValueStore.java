import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class NoSerdeInMemoryKeyValueStore<K, V> implements KeyValueStore<K, V> {

  private final Map<K, V> store = new HashMap<>();

  private final Serde<K> serdeKey;

  private final Serde<V> serdeValue;

  private final String name;

  public NoSerdeInMemoryKeyValueStore(String name, Serde<K> serdeKey, Serde<V> serdeValue) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(serdeKey);
    Objects.requireNonNull(serdeValue);

    this.name = name;
    this.serdeKey = serdeKey;
    this.serdeValue = serdeValue;
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      store.put(key, value);
    } else {
      store.remove(key);
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return store.putIfAbsent(key, value);
  }

  @Override
  public void putAll(List<KeyValue<K, V>> entries) {
    entries.stream()
        .parallel()
        .forEach(
            entry -> {
              store.put(entry.key, entry.value);
            });
  }

  @Override
  public V delete(K key) {
    return store.remove(key);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(StateStoreContext context, StateStore root) {
    context.register(
        root,
        new BatchingStateRestoreCallback() {
          @Override
          public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
            records.stream()
                .map(
                    keyValue ->
                        ObjectObjectImmutablePair.of(
                            serdeKey.deserializer().deserialize(null, keyValue.key),
                            serdeValue.deserializer().deserialize(null, keyValue.value)))
                .forEach(kv -> store.put(kv.key(), kv.value()));
          }
        });
  }

  @Override
  public void flush() {
    // empty by design
  }

  @Override
  public void close() {
    // empty by design
  }

  @Override
  public boolean persistent() {
    return false;
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public V get(K key) {
    return store.get(key);
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<K, V> all() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long approximateNumEntries() {
    return store.size();
  }
}
