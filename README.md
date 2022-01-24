A read-only in-memory key value store for Kafka Streams.

Unlike `Stores.inMemoryKeyValueStore` uses declared generic java data types for keys and values making it un-necessary to serialize key and deserialize value on every access.