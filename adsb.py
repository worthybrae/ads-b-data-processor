import socket
import sys
import subprocess
import time
import os
from datetime import datetime
import hashlib
import pandas as pd


def sbs_parser(string):
    cut_words = [
        'MSG',
        'CLK',
        'STA',
        'AIR',
        'ID',
        'SEL'
    ]
    cleaned_string = string.decode('utf-8').strip().replace(' ','').replace('\r', '').replace('\n', '')
    string_parts = cleaned_string.split(',')
    message_list = []
    if len(string_parts) > 0:
        current_message = []
        for part in string_parts:
            formatted_part = part.strip()
            if not current_message:
                current_message.append(formatted_part)
            elif formatted_part in cut_words:
                message_list.append(current_message)
                current_message = [formatted_part]
            else:
                current_message.append(formatted_part)
    return message_list



class ADSBData:
    def __init__(self, message):
        try:
            # Extract relevant data
            self.message_type = message[0]
            self.aircraft_type = message[3]
            self.aircraft_id = message[4]
            self.flight_id = message[5]
            self.timestamp = datetime.strptime(f"{message[8]} {message[9]}", "%Y/%m/%d %H:%M:%S.%f") 
            # Depending on the transmission type, other fields may be populated
            self.altitude = int(message[11]) if message[11] else None
            self.ground_speed = int(message[12]) if message[12] else None
            self.track = int(message[13]) if message[13] else None
            self.latitude = float(message[14]) if message[14] else None
            self.longitude = float(message[15]) if message[15] else None
            self.callsign = message[10].strip() if message[10] else None
            # Generate a unique ID for the data point (customize as needed)
            ifl_hash_str = f"{self.aircraft_id}{self.latitude:.4f}{self.longitude:.4f}{self.timestamp.strftime('%Y-%m-%d %H:%M')}"
            self.ifl_hash = hashlib.md5(ifl_hash_str.encode()).hexdigest()
            dup_hash_str = f"{self.aircraft_id}{self.latitude}{self.longitude}{self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}"
            self.dup_hash = hashlib.md5(dup_hash_str.encode()).hexdigest()
            self.ingested_at = datetime.utcnow()
        except:
            self.aircraft_id = None
            self.message_type = None
            self.latitude = None
            self.longitude = None
            self.timestamp = None

    def __str__(self):
        return f"Aircraft ID: {self.aircraft_id}, Latitude: {self.latitude}, Longitude: {self.longitude}, Timestamp: {self.timestamp}"

    def validate(self):
        if self.message_type == 'MSG' and self.aircraft_id and self.latitude and self.longitude and self.timestamp:
            return True
        else:
            return False

class ADSBDataStream:
    def __init__(self):
        self.messages = []
        self.last_hour = None
        self.last_date = None

    def add_new_message(self, message):
        self.messages.append([
            message.aircraft_id,
            message.aircraft_type,
            message.flight_id,
            message.timestamp,
            message.altitude,
            message.ground_speed,
            message.track,
            message.latitude,
            message.longitude,
            message.callsign,
            message.ifl_hash,
            message.dup_hash,
            message.ingested_at
        ])
        self.check_if_new_hour(message.timestamp.hour, message.timestamp.strftime('%Y-%m-%d'))

    def check_if_new_hour(self, hour, date):
        if self.last_hour is None:
            self.last_hour = int(hour)
            self.last_date = date
        
        elif int(hour) != self.last_hour:
            self.save_messages()
            self.last_hour = int(hour)
            self.last_date = date

    def save_messages(self):
        if not self.messages:
            return

        df = pd.DataFrame(self.messages, columns=[
            'aircraft_id',
            'aircraft_type',
            'flight_id',
            'timestamp',
            'altitude',
            'ground_speed',
            'track',
            'latitude',
            'longitude',
            'callsign',
            'ifl_hash',
            'dup_hash',
            'ingested_at'
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


def receiver():
    # run_dump1090() # use to make sure background process is feeding data to the socket
    while True:
        try:
            # Attempt to establish a connection
            aggregated_data = SocketConnection('out.adsb.lol', 1338, 'aggregated data feed')
            aggregated_data_stat, aggregated_data_conn = aggregated_data.create_connection()

            if aggregated_data_stat:
                print("Connection established to aggregated data feed.")
                stream = ADSBDataStream()

                while True:
                    data = aggregated_data_conn.recv(1024)
                    if not data:
                        raise ConnectionError("No data received. Connection might be lost.")
                    
                    message_list = sbs_parser(data)
                    if message_list:
                        for message in message_list:
                            adsb_obj = ADSBData(message)
                            if adsb_obj.validate():
                                print(adsb_obj)
                                stream.add_new_message(adsb_obj)

            else:
                # If connection was not successful, print an error and try to reconnect
                print("Failed to connect to the aggregated data feed. Retrying...")
                time.sleep(5)
                continue

        except (ConnectionError, socket.error) as e:
            print(f"Connection error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break  # Exit the loop if a non-recoverable error occurs

if __name__ == '__main__':
    receiver()