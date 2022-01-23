import com.jsoniter.any.Any;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class AnySerializer implements Serializer<Any> {

  @Override
  public byte[] serialize(String topic, Any data) {
    return data.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, Any data) {
    return Serializer.super.serialize(topic, headers, data);
  }
}
