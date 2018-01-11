#!/usr/bin/env python

# Python coding challenge

# Takes vlans.csv and requests.csv as input and produces output.csv that
# specifies which requests reserved which VLAN_IDs on which port and device.

import csv, sqlite3, argparse

# Set default input files
parser = argparse.ArgumentParser()
parser.add_argument('requests', nargs='?', const=1, default='requests.csv')
parser.add_argument('vlans', nargs='?', const=1, default='vlans.csv')
args = parser.parse_args()

# Database connection
con = sqlite3.connect(":memory:")
con.row_factory = sqlite3.Row
cur = con.cursor()

# Create requests schema
cur.execute("""\
    CREATE TABLE requests (
        id INTEGER PRIMARY KEY NOT NULL ,
        request_id int NOT NULL ,
        redundant tinyint(1) NOT NULL
    );
""")

cur.execute("""\
    CREATE UNIQUE INDEX unique_requests
    ON requests (request_id)
""")

# Create vlans schema
cur.execute("""\
    CREATE TABLE vlans (
        id INTEGER PRIMARY KEY NOT NULL ,
        device_id int NOT NULL ,
        primary_port tinyint(1) NOT NULL ,
        vlan_id int NOT NULL ,
        reserved tinyint(1) DEFAULT 0
    );
""")

cur.execute("""\
    CREATE UNIQUE INDEX unique_vlans
    ON vlans (vlan_id, device_id, primary_port)
""")

con.commit()

# Load vlans.csv
with open(args.vlans, 'rb') as vlan_handle:
    reader = csv.DictReader(vlan_handle)
    all_rows = [(row['device_id'], row['primary_port'], row['vlan_id'])
                for row in reader]

cur.executemany(
    "INSERT INTO vlans (device_id, primary_port, vlan_id) VALUES (?, ?, ?);",
    all_rows)

con.commit()

# Load requests.csv
with open(args.requests, 'rb') as request_handle:
    reader = csv.DictReader(request_handle)
    all_rows = [(row['request_id'], row['redundant']) for row in reader]

cur.executemany("INSERT INTO requests (request_id, redundant) VALUES (?, ?);",
                all_rows)

con.commit()

# Create output file with header
writer = csv.writer(open("./output.csv", 'w'))
writer.writerow(['request_id', 'device_id', 'primary_port', 'vlan_id'])

# Request functions
def normal_request(request_id):
    c = con.cursor()
    c.execute("""
        SELECT id, device_id, primary_port, vlan_id
        FROM vlans
        WHERE primary_port = 1
        AND reserved = 0
        ORDER BY vlan_id, device_id ASC
        LIMIT 1
        """)
    row = c.fetchone()
    c.execute("UPDATE vlans SET reserved=? WHERE id=?", (1, row['id']))
    writer.writerow(
        [request_id, row['device_id'], row['primary_port'], row['vlan_id']])


def redundant_request(request_id):
    c = con.cursor()
    c.execute("""
        SELECT vlan_id, device_id
        FROM vlans
        WHERE reserved = 0
        GROUP BY device_id, vlan_id
        HAVING COUNT(primary_port) = 2
        ORDER BY vlan_id ASC, device_id ASC
        LIMIT 1
        """)
    row = c.fetchone()
    c.execute("UPDATE vlans SET reserved=? WHERE vlan_id=? AND device_id=?",
              (1, row['vlan_id'], row['device_id']))
    for i in range(2):
        writer.writerow([request_id, row['device_id'], i, row['vlan_id']])

# Route requests
cur.execute("""\
    SELECT request_id, redundant
    FROM requests
    ORDER BY request_id ASC
""")

while True:
    row = cur.fetchone()
    if row == None:
        break
    elif row["redundant"]:
        redundant_request(row['request_id'])
    else:
        normal_request(row['request_id'])

con.close()
