# This kafka-python client needs to 'pip install kafka-python'
import time
from kafka import KafkaProducer

kafka_host = '10.42.0.1'  # host
kafka_port = 50088  # port
kafka_topic = 'hello'  # topic

producer = KafkaProducer(
    client_id='hengP',
    bootstrap_servers=['{kafka_host}:{kafka_port}'.format(kafka_host=kafka_host, kafka_port=kafka_port)]
                         )

message_string = 'some_message_' + str(time.time())
response = producer.send(kafka_topic, message_string.encode('utf-8'))
print message_string + ' has been sent.'

# producer = KafkaProducer(client_id='hengP', bootstrap_servers='10.42.0.1:50088')
#
# while True:
#     producer.send('hello', 'message_' + str(time.time()))
#     # need gbk decode
#     producer.send('hello', b"\xc2Hola, mundo!")
#     time.sleep(1)
