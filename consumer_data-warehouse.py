from kafka import KafkaConsumer
import mysql.connector
import json

# Désérialisation JSON
def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

# Connexion MySQL
def connect_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='jai changer le mot de passe',
        database='data_warehouse'
    )

# Fonction d'insertion dynamique par topic
def insert_data(cursor, topic, data, key):
    if topic == "TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD":
        query = """
            INSERT INTO TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD (PAYMENT_METHOD, TOTAL_AMOUNT)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE total_amount = VALUES(total_amount)
        """
        decode = key.decode('utf-8')
        parts = decode.rsplit('-', 0)
        values = (parts[1], data['PAYMENT_METHOD'])

    elif topic == "COUNT_NUMB_BUY_PER_PRODUCT":
        query = """
            INSERT INTO COUNT_NUMB_BUY_PER_PRODUCT (PRODUCT_ID, TOTAL_QUANTITY)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE total_quantity = VALUES(total_quantity)
        """
        decode = key.decode('utf-8')
        parts = decode.rsplit('-', 1)
        values = (parts[1], data['TOTAL_QUANTITY'])

    elif topic == "TOTAL_SPENT_PER_USER_TRANSACTION_TYPE":
        query = """
            INSERT INTO TOTAL_SPENT_PER_USER_TRANSACTION_TYPE (USER_TRANSACTION_KEY, TOTAL_SPENT)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE total_spent = VALUES(total_spent)
        """
        # On décode et découpe la clé composite "user_id_hashed-transaction_type"
        decode = key.decode('utf-8')
        parts = decode.rsplit('-', 1)
        values = (parts[1], data['total_spent'])

    elif topic == "TRANSACTION_STATUS_EVOLUTION":
        query = """
            INSERT INTO TRANSACTION_STATUS_EVOLUTION (TRANSACTION_ID, LATEST_STATUS)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE latest_status = VALUES(latest_status)
        """
        decode = key.decode('utf-8')
        parts = decode.rsplit('-', 1)
        values = (parts[1], data['latest_status'])

    elif topic == "AMOUNT_PER_TYPE_WINDOWED":
        query = """
            INSERT INTO amount_per_type_windowed (TRANSACTION_TYPE, WINDOW_START, WINDOW_END, TOTAL_AMOUNT)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE total_amount = VALUES(total_amount)
        """
        decode = key.decode('utf-8')
        parts = decode.rsplit('-', 1)
        values = (
            parts[0],
            data['window_start'],
            data['window_end'],
            data['total_amount']
        )

    else:
        raise ValueError(f"Topic non géré : {topic}")

    cursor.execute(query, values)

if __name__ == '__main__':
    db = connect_mysql()
    cursor = db.cursor()

    # Liste des topics Kafka que tu consommes
    topics = [
        "TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD",
        "COUNT_NUMB_BUY_PER_PRODUCT",
        "TOTAL_SPENT_PER_USER_TRANSACTION_TYPE",
        "TRANSACTION_STATUS_EVOLUTION",
        "AMOUNT_PER_TYPE_WINDOWED"
    ]

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='mysql-ingestor',
        value_deserializer=json_deserializer
    )

    print("Consumer MySQL en écoute...")

    for message in consumer:
        topic = message.topic
        key = message.key
        data = message.value
        try:
            #nsert_data(cursor, topic, data)
            #db.commit()
            #print(f"[{topic}] Inséré : {data}")
            print(f"Reçu depuis {topic} : {data} ,, {key}")
            insert_data(cursor, topic, data, key)
        except Exception as e:
            print(f"Erreur pour topic {topic} : {e}")
            db.rollback()

    cursor.close()
    db.close()