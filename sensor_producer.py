from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random
from datetime import datetime

# Ваш ідентифікатор для назви топіка
my_name = "rsm_goit"
topic_name = f"{my_name}_building_sensors"

# Ініціалізуємо продюсер Kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Генеруємо фіксований ID датчика для цього запуску
sensor_id = random.randint(1000, 9999)
print(f"Starting sensor with ID: {sensor_id}")

try:
    # Безкінечний цикл для імітації роботи датчика
    while True:
        # Генеруємо випадкові дані
        temperature = round(random.uniform(25.0, 45.0), 1)
        humidity = round(random.uniform(15.0, 85.0), 1)

        # Поточний час
        timestamp = datetime.now().isoformat()

        # Формуємо повідомлення
        message = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "humidity": humidity
        }

        # Відправляємо повідомлення в Kafka
        producer.send(topic_name, message)
        producer.flush()

        print(f"Sent: {message}")

        # Затримка перед наступним вимірюванням (5 секунд)
        time.sleep(5)

except KeyboardInterrupt:
    print("Sensor stopped by user")
finally:
    # Закриваємо продюсер
    producer.close()
    print("Kafka producer closed")