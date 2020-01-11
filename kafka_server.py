import producer_server


def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    print("Creating producer")
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.sf.crimedata",
        bootstrap_servers="localhost:9092",
        client_id="0"
    )
    print("Producer created successfully")
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
