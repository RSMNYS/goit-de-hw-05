from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення ідентифікатора користувача
my_name = "rsm_goit"

# Визначення нових топіків
topics_to_create = [
    f"{my_name}_building_sensors",
    f"{my_name}_temperature_alerts",
    f"{my_name}_humidity_alerts"
]

# Налаштування для топіків
num_partitions = 2
replication_factor = 1

# Створення топіків
new_topics = [
    NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
    for topic in topics_to_create
]

# Створення нових топіків
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Topics created successfully:")
    for topic in topics_to_create:
        print(f"- {topic}")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків з нашим ідентифікатором
print("\nList of topics with your identifier:")
[print(topic) for topic in admin_client.list_topics() if my_name in topic]

# Закриття зв'язку з клієнтом
admin_client.close()