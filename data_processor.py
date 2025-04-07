from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
from datetime import datetime

# Ваш ідентифікатор для назви топіків
my_name = "rsm_goit"  # Замініть yourname на ваше ім'я або інший ідентифікатор

# Назви топіків
sensors_topic = f"{my_name}_building_sensors"
temp_alerts_topic = f"{my_name}_temperature_alerts"
humidity_alerts_topic = f"{my_name}_humidity_alerts"

# Порогові значення для сповіщень
TEMP_THRESHOLD = 40.0  # °C
HUMIDITY_HIGH_THRESHOLD = 80.0  # %
HUMIDITY_LOW_THRESHOLD = 20.0  # %

# Ініціалізуємо споживач Kafka
consumer = KafkaConsumer(
    sensors_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=f"{my_name}_processor_group",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Ініціалізуємо продюсер Kafka для відправки сповіщень
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting to process data from topic: {sensors_topic}")
print(f"Will send alerts to: {temp_alerts_topic} and {humidity_alerts_topic}")

# Обробка вхідних повідомлень
for message in consumer:
    # Отримуємо дані з повідомлення
    data = message.value

    # Виводимо отримані дані
    print(f"Received data: {data}")

    # Перевіряємо температуру
    if data['temperature'] > TEMP_THRESHOLD:
        alert = {
            "sensor_id": data['sensor_id'],
            "timestamp": data['timestamp'],
            "alert_time": datetime.now().isoformat(),
            "temperature": data['temperature'],
            "message": f"High temperature alert! Temperature {data['temperature']}°C exceeds threshold of {TEMP_THRESHOLD}°C"
        }

        # Відправляємо сповіщення про температуру
        producer.send(temp_alerts_topic, alert)
        producer.flush()
        print(f"Temperature alert sent: {alert}")

    # Перевіряємо вологість
    if data['humidity'] > HUMIDITY_HIGH_THRESHOLD or data['humidity'] < HUMIDITY_LOW_THRESHOLD:
        # Визначаємо тип сповіщення (висока/низька вологість)
        if data['humidity'] > HUMIDITY_HIGH_THRESHOLD:
            message_text = f"High humidity alert! Humidity {data['humidity']}% exceeds threshold of {HUMIDITY_HIGH_THRESHOLD}%"
        else:
            message_text = f"Low humidity alert! Humidity {data['humidity']}% is below threshold of {HUMIDITY_LOW_THRESHOLD}%"

        alert = {
            "sensor_id": data['sensor_id'],
            "timestamp": data['timestamp'],
            "alert_time": datetime.now().isoformat(),
            "humidity": data['humidity'],
            "message": message_text
        }

        # Відправляємо сповіщення про вологість
        producer.send(humidity_alerts_topic, alert)
        producer.flush()
        print(f"Humidity alert sent: {alert}")