from kafka import KafkaConsumer
import json
import os
from datetime import datetime

# Fonction pour désérialiser les messages JSON
def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

# Charge le fichier pour les topics (append ou overwrite)
def load_config():
    with open('feeds_config.json', 'r') as f:
        return json.load(f)

# Fonction pour importer les données dans un datalake
def write_to_data_lake(topic_name, message_value):
    # Créer le chemin basé sur la date
    today = datetime.today().strftime('%Y-%m-%d')
    directory_path = os.path.join('data_lake', topic_name, today)
    os.makedirs(directory_path, exist_ok=True)

    # Nom du fichier
    file_path = os.path.join(directory_path, 'data.json')

    # Écriture en mode append
    with open(file_path, 'a', encoding='utf-8') as f:
        json.dump(message_value, f, ensure_ascii=False)
        f.write('\n')  # Un message JSON par ligne

if __name__ == '__main__':
    # Créer un Kafka Consumer
    config = load_config()
    topic_list = list(config['topics'].keys())
    consumer = KafkaConsumer(
        *topic_list,  # Topic à consommer
        bootstrap_servers=['localhost:9092'],  # Adresse du broker Kafka
        auto_offset_reset='earliest',  # Lire les messages depuis le début du topic
        enable_auto_commit=True,  # Confirme automatiquement les messages lus
        group_id='data-lake-consumer-group',  # Groupe de consommateurs
        value_deserializer=json_deserializer  # Désérialisation en JSON
    )
    # Consommer les messages du topic
    print("En attente de messages...")

    for message in consumer:
        current_timestamp = datetime.now().timestamp()
        topic = message.topic
        message_value = message.value
        message_ts = message_value.get("timestamp")
        ts_delta = (current_timestamp - message_ts) if message_ts is not None else -1
        #print(f"Message reçu: {message.value}")
        print(f"Message reçu: {message.value} with processing time {ts_delta} Seconds")
        write_to_data_lake(topic, message_value)