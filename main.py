from mqtt_client import MQTTClient

def main():
    broker = 'iot.coreflux.cloud'
    port = 8883

    mqtt_client = MQTTClient(broker, port)
    
    try:
        mqtt_client.run()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        mqtt_client.disconnect()
        print("Application terminated.")

if __name__ == "__main__":
    main()
