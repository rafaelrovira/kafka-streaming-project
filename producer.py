from kafka import KafkaProducer
import json
import time

print("Iniciando producer...")

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(1, 20):
    message = {
        "order_id": i,
        "sales": round(100 + i * 10.5, 2)
    }
    producer.send("orders_topic", value=message)
    producer.flush()
    print(f"Enviado: {message}")
    time.sleep(2)

print("Producer finalizado.")