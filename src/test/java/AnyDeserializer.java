import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class AnyDeserializer implements Deserializer<Any> {

  @Override
  public Any deserialize(String topic, byte[] data) {
    return data == null ? null : JsonIterator.deserialize(data);
  }

  @Override
  public Any deserialize(String topic, Headers headers, byte[] data) {
    return deserialize(topic, data);
  }
}
