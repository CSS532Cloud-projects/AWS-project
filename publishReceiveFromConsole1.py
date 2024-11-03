from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
import json

endpoint = "myEndPoint"
client_id = "myClient"
receive_topic = "myTopic1"  
response_topic = "myTopic2"  
message_count = 2  
received_count = 0
received_all_event = threading.Event()

def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    global received_count
    print("Received message from topic '{}': {}".format(topic, payload.decode('utf-8')))
    received_count += 1

    try:
        command = json.loads(payload)
        processed_message = f"Processed command: {command}"
        print(processed_message)

        response = {
            "original_command": command,
            "status": "success",
            "message": "Command processed successfully."
        }
        
        mqtt_connection.publish(
            topic=response_topic,
            payload=json.dumps(response),
            qos=mqtt.QoS.AT_LEAST_ONCE
        )
        print("Published response to topic '{}': {}".format(response_topic, response))

    except json.JSONDecodeError:
        print("Received non-JSON message, unable to process.")

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

print("Subscribing to command topic '{}'...".format(receive_topic))
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=receive_topic,
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received)
subscribe_result = subscribe_future.result()
print("Subscribed with {}".format(str(subscribe_result['qos'])))

print("Waiting for messages...")
received_all_event.wait()

print("{} message(s) received.".format(received_count))

print("Disconnecting...")
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
print("Disconnected!")
