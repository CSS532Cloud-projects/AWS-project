from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
import json

endpoint="myEndPoint"
client_id="myClient"
topic="myTopic1"

message_count = 2
message_string = f"Hello from {client_id}"

received_count = 0
received_all_event = threading.Event()

def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    if received_count == message_count:
        received_all_event.set()

mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath="certificate",
        pri_key_filepath="privateKey",
        ca_filepath="root",
        client_id=client_id,
        clean_session=False)

print(f"Connecting {endpoint} to endpoint with client ID '{client_id}'...")

connect_future = mqtt_connection.connect()
connect_future.result()
print("Connected!")

print("Subscribing to topic '{}'...".format(topic))
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=topic,
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received)
subscribe_result = subscribe_future.result()
print("Subscribed with {}".format(str(subscribe_result['qos'])))

print("Sending {} message(s)".format(message_count))
publish_count = 1
while (publish_count <= message_count):
    message = "{} [{}]".format(message_string, publish_count)
    print("Publishing message to topic '{}': {}".format(topic, message))
    message_json = json.dumps(message)
    
    mqtt_connection.publish(
        topic=topic,
        payload=message_json,
        qos=mqtt.QoS.AT_LEAST_ONCE)
    time.sleep(1)
    publish_count += 1

received_all_event.wait()
print("{} messages(s) received.".format(received_count))

print("Disconnecting...")
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
print("Disconnected!")