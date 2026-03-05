from kafka import KafkaConsumer
import json

print("Iniciando consumer...")

consumer = KafkaConsumer(
    "orders_topic",
    bootstrap_servers="localhost:9093",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="orders-consumer-group"
)

print("Aguardando mensagens...")

for message in consumer:
    print(f"Mensagem recebida: {message.value}")