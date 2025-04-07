from kafka import KafkaConsumer
from configs import kafka_config
import json
import threading

# Ваш ідентифікатор для назви топіків
my_name = "rsm_goit"

# Назви топіків сповіщень
temp_alerts_topic = f"{my_name}_temperature_alerts"
humidity_alerts_topic = f"{my_name}_humidity_alerts"


# Функція для створення та запуску споживача для конкретного топіку
def create_consumer(topic, name):
    # Створення споживача
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=f"{my_name}_alerts_group_{name}",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Started {name} consumer for topic: {topic}")

    # Обробка вхідних повідомлень
    for message in consumer:
        alert_data = message.value
        print(f"\n===== {name.upper()} ALERT =====")
        print(f"Sensor ID: {alert_data['sensor_id']}")
        print(f"Timestamp: {alert_data['timestamp']}")
        print(f"Alert Time: {alert_data['alert_time']}")

        # Виводимо специфічні дані в залежності від типу сповіщення
        if name == "temperature":
            print(f"Temperature: {alert_data['temperature']}°C")
        else:
            print(f"Humidity: {alert_data['humidity']}%")

        print(f"Message: {alert_data['message']}")
        print("=" * 30)


# Створюємо потоки для паралельного зчитування з обох топіків
temp_thread = threading.Thread(
    target=create_consumer,
    args=(temp_alerts_topic, "temperature"),
    daemon=True
)

humidity_thread = threading.Thread(
    target=create_consumer,
    args=(humidity_alerts_topic, "humidity"),
    daemon=True
)

# Запускаємо потоки
temp_thread.start()
humidity_thread.start()

print("Alert monitoring started. Press Ctrl+C to stop.")

try:
    # Очікуємо завершення потоків (що ніколи не відбудеться, але можна перервати Ctrl+C)
    temp_thread.join()
    humidity_thread.join()
except KeyboardInterrupt:
    print("\nAlert monitoring stopped by user")