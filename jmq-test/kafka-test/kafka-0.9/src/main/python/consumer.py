# This kafka-python client needs to 'pip install kafka-python'

from kafka import KafkaConsumer

kafka_host = '10.42.0.1'  # host
kafka_port = 50088  # port
kafka_topic = 'hello'  # topic
kafka_group = 'helloPython'  # group id

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(kafka_topic,
                         group_id=kafka_group,
                         bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
                             kafka_host=kafka_host,
                             kafka_port=kafka_port
                         )])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode('utf-8')))

# consumer = KafkaConsumer(auto_offset_reset='earliest',
#                          bootstrap_servers=
#                          ['{kafka_host}:{kafka_port}'.format(kafka_host=kafka_host, kafka_port=kafka_port)])
# consumer.subscribe(['hello'])
#
# for message in consumer:
#     print (message)