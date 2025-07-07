import socket
import mysql.connector

# Set up database connection
db = mysql.connector.connect(
    host="mysql.railway.internal",
    port=3306,
    user="root",
    password="tdrryrozlbjenycipyluVbdloKixuiKb",
    database="railway"
)

cursor = db.cursor()

def save_tracker_data(data):
    # Check if tracker with given IMEI exists
    cursor.execute("SELECT id FROM trackers WHERE imei = %s", (data['imei'],))
    result = cursor.fetchone()

    if result:
        # If tracker exists, update it
        sql = """
        UPDATE trackers
        SET 
            model = %s,
            event_status = %s,
            battery_voltage = %s,
            `utc_time` = %s,
            gps_status = %s,
            latitude = %s,
            longitude = %s,
            `date` = %s
        WHERE imei = %s
        """

        values = (
            data['model'],
            data['event_status'],
            data['battery_voltage'],
            data['gps']['utc_time'],
            data['gps']['status'],
            data['gps']['latitude'],
            data['gps']['longitude'],
            data['gps']['date'],
            data['imei']
        )
        cursor.execute(sql, values)
        db.commit()
        print("✅ Data updated.")
    else:
        print("⚠️ IMEI not found in DB. Skipping insert.")

# Function to parse raw data from tracker
def parse_data(raw_data):
    lines = raw_data.strip().split('\n')
    header = lines[0].split('#')
    imei = header[1]
    model = header[2]
    event_status = header[4]

    battery_line = lines[1].split('$')[0].replace('#', '')
    battery_voltage = float(battery_line) / 1000  # Convert mV to volts

    gps_data = lines[1].split('$')[1].split(',')

    parsed = {
        'imei': imei,
        'model': model,
        'event_status': event_status,
        'battery_voltage': battery_voltage,
        'gps': {
            'utc_time': gps_data[1],
            'status': gps_data[2],
            'latitude': gps_data[3] + gps_data[4],
            'longitude': gps_data[5] + gps_data[6],
            'date': gps_data[9],
        }
    }
    return parsed

# Server listening for connections and data
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(('0.0.0.0', 7700))
    s.listen()
    print("🚀 Server listening on port 7700...")
    while True:
        conn, addr = s.accept()
        with conn:
            print('🔌 Connected by', addr)
            data = conn.recv(1024).decode()
            if not data:
                continue

            parsed_data = parse_data(data)
            print("📡 Parsed data:", parsed_data)
            save_tracker_data(parsed_data)
