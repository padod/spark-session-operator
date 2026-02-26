"""
Test script for Thrift HTTP transport via the Spark Session Operator proxy.

Usage from Jupyter notebook - copy the cells below.

Prerequisites:
    pip install pyhive thrift
"""

# %% Configuration
THRIFT_HOST = "spark-thrift-default.fnr-dev-i.corp.tander.ru"
THRIFT_PORT = 80  # nginx ingress HTTP port
USERNAME = "your_username"  # Keycloak username
PASSWORD = "your_password"  # Keycloak password

# %% Connect via PyHive (HTTP transport)
import base64
from pyhive import hive
from thrift.transport import THttpClient

# PyHive has no built-in HTTP transport mode parameter.
# We construct a THttpClient manually and pass it via thrift_transport.
http_uri = f"http://{THRIFT_HOST}:{THRIFT_PORT}/cliservice"
transport = THttpClient.THttpClient(http_uri)

# Set Basic auth header (the proxy expects this for Keycloak ROPC exchange)
credentials = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
transport.setCustomHeaders({
    "Authorization": f"Basic {credentials}",
})

conn = hive.connect(thrift_transport=transport)
cursor = conn.cursor()
print(f"Connected to {THRIFT_HOST}")

# %% Smoke test
cursor.execute("SELECT 1 AS test_value")
print("Smoke test:", cursor.fetchall())
# Expected: [(1,)]

# %% List databases
cursor.execute("SHOW DATABASES")
print("Databases:")
for row in cursor.fetchall():
    print(f"  {row[0]}")

# %% Run a query (uncomment and adjust)
# cursor.execute("SELECT * FROM your_database.your_table LIMIT 10")
# for row in cursor.fetchall():
#     print(row)

# %% Cleanup
cursor.close()
conn.close()
print("Done.")
