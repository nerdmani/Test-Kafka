from confluent_kafka import Producer, Consumer

# Configurações do Kafka
conf = {'bootstrap.servers': "localhost:9092"}

# Função para enviar mensagens (produtor)
def produce(topic, message):
    p = Producer(**conf)
    p.produce(topic, message.encode('utf-8'))
    p.flush()

# Função para receber mensagens (consumidor)
def consume(topic):
    c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup',
                  'auto.offset.reset': 'earliest'})
    c.subscribe([topic])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

# Exemplo de uso 
if __name__ == "__main__":
    topic = "Testes"
    message = "Hello, Kafka!"
    produce(topic, message)
    consume(topic)
