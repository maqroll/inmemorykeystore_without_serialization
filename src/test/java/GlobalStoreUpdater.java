import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class GlobalStoreUpdater<K, V> implements Processor<K, V, Void, Void> {

  private final String storeName;

  private KeyValueStore<K, V> store;

  public GlobalStoreUpdater(final String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(final ProcessorContext<Void, Void> processorContext) {
    store = processorContext.getStateStore(storeName);
  }

  @Override
  public void process(final Record<K, V> record) {
    store.put(record.key(), record.value());
  }

  @Override
  public void close() {
    // No-op
  }
}
