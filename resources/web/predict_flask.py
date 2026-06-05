import sys, os, re
from flask import Flask, render_template, request
from bson import json_util

# Configuration details
import config

# Helpers for search and prediction APIs
import predict_utils

# Set up Flask
app = Flask(__name__)

import json

# Date/time stuff
import iso8601
import datetime

# Setup Kafka
from kafka import KafkaProducer, KafkaConsumer
KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
producer = KafkaProducer(bootstrap_servers=[f'{KAFKA_HOST}:9092'], api_version=(4,2,0))
PREDICTION_TOPIC = 'flight-delay-ml-request'
RESPONSE_TOPIC = 'flight-delay-ml-response'

# Setup SocketIO
from flask_socketio import SocketIO, join_room
socketio = SocketIO(app)

# Background Kafka consumer thread — restarts automatically on any error
import threading
import time

def kafka_response_consumer():
    while True:
        try:
            print(f"[kafka_consumer] Connecting to {KAFKA_HOST}:9092 topic={RESPONSE_TOPIC}", flush=True)
            consumer = KafkaConsumer(
                RESPONSE_TOPIC,
                bootstrap_servers=[f'{KAFKA_HOST}:9092'],
                api_version=(4, 2, 0),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("[kafka_consumer] Consumer ready, waiting for predictions...", flush=True)
            for message in consumer:
                prediction = message.value
                uuid = prediction.get('UUID')
                print(f"[kafka_consumer] Got prediction UUID={uuid} Prediction={prediction.get('Prediction')}", flush=True)
                if uuid:
                    socketio.emit('prediction', prediction, room=uuid, namespace='/')
        except Exception as exc:
            print(f"[kafka_consumer] ERROR: {exc} — restarting in 3s", flush=True)
            time.sleep(3)

consumer_thread = threading.Thread(target=kafka_response_consumer, daemon=True)
consumer_thread.start()

@socketio.on('join')
def on_join(data):
    uuid = data['uuid']
    join_room(uuid)

import uuid

project_home = os.environ["PROJECT_HOME"]

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  """POST API for classifying flight delays"""

  # Define the form fields to process
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }

  # Fetch the values for each field from the form object
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)

  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value

  # Set the derived values from Cassandra
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    api_form_values['Origin'],
    api_form_values['Dest']
  )

  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value

  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()

  # Create a unique ID for this message
  unique_id = str(uuid.uuid4())
  prediction_features['UUID'] = unique_id

  message_bytes = json.dumps(prediction_features).encode()
  producer.send(PREDICTION_TOPIC, message_bytes)

  response = {"status": "OK", "id": unique_id}
  return json_util.dumps(response)

@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
  """Serves flight delay prediction page with websocket"""

  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'}
  ]

  return render_template('flight_delays_predict_kafka.html', form_config=form_config)

@app.route('/health')
def health():
  return 'OK', 200


@app.route('/shutdown')
def shutdown():
  return 'Server shutting down...'

if __name__ == "__main__":
    socketio.run(app, debug=True, host='0.0.0.0', port=5001, allow_unsafe_werkzeug=True)
