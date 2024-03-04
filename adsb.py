import socket
import sys
import subprocess
import time
import json
import os
from datetime import datetime
import hashlib
import pandas as pd
import threading


class ADSBData:
    def __init__(self, adsb_message_bytes):
        # Decode byte string to string
        message_str = adsb_message_bytes.decode('utf-8').strip()
        # Split the message by commas to parse the fields
        message_parts = message_str.split(',')

        # Extract relevant data
        self.message_type = message_parts[0]
        self.transmission_type = message_parts[1]
        self.session_id = message_parts[2]
        self.aircraft_id = message_parts[3]
        self.hex_ident = message_parts[4]
        self.flight_id = message_parts[5]
        self.date_message_generated = message_parts[6]
        self.time_message_generated = message_parts[7]
        self.date_message_logged = message_parts[8]
        self.time_message_logged = message_parts[9]
        # Depending on the transmission type, other fields may be populated
        self.altitude = int(message_parts[11]) if message_parts[11] else None
        self.latitude = float(message_parts[14]) if message_parts[14] else None
        self.longitude = float(message_parts[15]) if message_parts[15] else None
        self.callsign = message_parts[10].strip() if message_parts[10] else None
        # Generate a unique ID for the data point (customize as needed)
        self.unique_id = hashlib.md5(f"{self.hex_ident}{self.date_message_logged}{self.time_message_logged}".encode()).hexdigest()

    def __str__(self):
        return f"Hex Ident: {self.hex_ident}, Callsign: {self.callsign}, Altitude: {self.altitude}, Latitude: {self.latitude}, Longitude: {self.longitude}"

class ADSBDataStream:
    def __init__(self):
        self.messages = []
        self.last_hour = None

    def add_new_message(self, message):
        self.messages.append([
            message.flight_number,
            message.altitude,
            message.speed,
            message.latitude,
            message.longitude,
            message.timestamp,
            message.unique_id
        ])
        self.check_if_new_hour(message.timestamp.hour, message.timestamp.strftime('%Y-%m-%d'))

    def check_if_new_hour(self, hour, date):
        if self.last_hour is None or int(hour) != self.last_hour:
            self.save_messages()
            self.last_hour = int(hour)
            self.last_date = date

    def save_messages(self):
        if not self.messages:
            return

        df = pd.DataFrame(self.messages, columns=[
            'flight_number', 
            'altitude', 
            'speed', 
            'latitude', 
            'longitude', 
            'timestamp',
            'unique_id'
        ])

        year, month, day = self.last_date.split('-')
        hour = self.last_hour

        dir_path = os.path.join('data', 'adsb', year, month, day)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        file_path = os.path.join(dir_path, f"{hour}.csv.gz")
        df.to_csv(file_path, mode='a', index=False, compression='gzip')
        self.messages = []


class SocketConnection:

    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name

    def create_connection(self):
        adsbhub_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            adsbhub_sock.connect((self.host, self.port))
            print(f"Connected to {self.name}")
            return True, adsbhub_sock
        except socket.error as e:
            print(f"Could not connect to {self.name}: {e}")
            return False, None
        
def run_dump1090():
    try:
        print("Starting dump1090-mutability...")
        subprocess.Popen(["dump1090-mutability", "--interactive", "--net"])
        time.sleep(5)
    except Exception as e:
        print(f"Failed to start dump1090-mutability: {e}")
        sys.exit(1)

def receive_aggregated_data(connection):
    while True:
        data = connection.recv(1024)  # Receive aggregated data
        row = ADSBData(data)
        print(f"Received aggregated data: {row}")
    connection.close()
    print("Disconnected from aggregated data feed.")

def run():
    run_dump1090()

    # Connect to ADS-B broadcaster
    adsbhub = SocketConnection('data.adsbhub.org', 5001, 'ads-b broadcaster')
    adsbhub_stat, adsbhub_conn = adsbhub.create_connection()

    # Connect to ADS-B receiver
    dump1090 = SocketConnection('localhost', 30003, 'ads-b receiver')
    dump1090_stat, dump1090_conn = dump1090.create_connection()

    # Connect to aggregated data feed
    aggregated_data = SocketConnection('data.adsbhub.org', 5002, 'aggregated data feed')
    aggregated_data_stat, aggregated_data_conn = aggregated_data.create_connection()

    # Handle aggregated data in a separate thread
    if aggregated_data_stat:
        threading.Thread(target=receive_aggregated_data, args=(aggregated_data_conn,), daemon=True).start()

    # Handle local ADS-B data and forward to ADS-B hub
    if adsbhub_stat and dump1090_stat:
        while True:
            data = dump1090_conn.recv(1024)  # Receive data from dump1090
            if not data:
                break
            adsbhub_conn.sendall(data)  # Forward data to ADS-B Hub
    else:
        print(f"Error connecting to services...")

    # Cleanup
    dump1090_conn.close()
    adsbhub_conn.close()
    print("Disconnected from ADS-B broadcaster and receiver.")

if __name__ == '__main__':
    run()