import json
import paho.mqtt.client as mqtt
import ssl
import time
import threading
from data_aggregator import DataAggregator



class MQTTClient:
    def __init__(self, broker, port):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.data_aggregator = DataAggregator()

        # * This is a schedule to provide publication of data every 15 seconds *
        self.publish_timer = threading.Timer(15, self.publish_data)
        self.publish_timer.start()

        # Configure TLS connection without certificate validation
        self.client.tls_set(cert_reqs=ssl.CERT_NONE)  # provide either ssl.CERT_REQUIRED and load the certificates
        self.client.tls_insecure_set(True)  # accept validation 
        self.client.connect(broker, port, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
        # Subscribe to all relevant topics using wildcards
        client.subscribe("Coreflux/+/+/+/status/switch:0")

    def on_message(self, client, userdata, msg):
        print(f"Received message on {msg.topic} with TLS encryption")
        # Pass both the message payload and the topic
        
        self.data_aggregator.aggregate(msg.topic,json.loads(msg.payload.decode()))


    def publish_data(self):
        if self.data_aggregator.has_new_data():
            # Iterate through each location and publish the aggregated data
            for location, data in self.data_aggregator.data_storage.items():
                message = {
                    'timestamp': int(time.time() * 1000),  # Current time in milliseconds
                    'total_energy': data['total_energy']
                }
                # Assuming the location is formatted as 'City/Place', provide the default location and replace the else
                parts = location.split('/')
                city = parts[0] if len(parts) > 0 else 'Ermesinde'
                place = parts[1] if len(parts) > 1 else 'GatoFedorentoBar'
                # Publish using the unified namespace format
                self.client.publish(f"Coreflux/{city}/{place}/aggregated/energy", json.dumps(message))
                print(f"Published aggregated data for {location}: {message}")
            self.data_aggregator.clear_new_data_flag()
        self.publish_timer = threading.Timer(15, self.publish_data)
        self.publish_timer.start()

    def disconnect(self):
        self.client.disconnect()

    def run(self):
        try:
            while True:
                time.sleep(1)  # Sleeps 1 second and goes again
        except KeyboardInterrupt:
            print("Exiting application...")
            self.disconnect()
