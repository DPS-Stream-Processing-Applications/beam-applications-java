from kafka import KafkaConsumer, KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=["localhost:9093"],
    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
)

message="1443033000,872407767100,1453828788044"
try:
    producer.send("test", value=message)
except Exception as e:
    print(e)
