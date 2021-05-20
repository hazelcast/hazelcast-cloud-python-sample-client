import hazelcast
import logging
import random
from hazelcast.discovery import HazelcastCloudDiscovery
from os.path import abspath

"""
This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
After successful connection, it puts random entries into the map.

See: https://docs.hazelcast.cloud/docs/python-client
"""

logging.basicConfig(level=logging.INFO)
HazelcastCloudDiscovery._CLOUD_URL_BASE = "YOUR_DISCOVERY_URL"
client = hazelcast.HazelcastClient(
    cluster_name="YOUR_CLUSTER_NAME",
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    statistics_enabled=True,
    ssl_enabled=True,
    ssl_cafile=abspath("ca.pem"),
    ssl_certfile=abspath("cert.pem"),
    ssl_keyfile=abspath("key.pem"),
    ssl_password="YOUR_SSL_PASSWORD",
)

my_map = client.get_map("map").blocking()
my_map.put("key", "value")

if my_map.get("key") == "value":
    print("Successfully connected!")
    print("Now, 'map' will be filled with random entries.")

    iteration_counter = 0
    while True:
        random_key = random.randint(1, 100000)
        random_key_str = str(random_key)
        my_map.put("key" + random_key_str, "value" + random_key_str)
        my_map.get("key" + random_key_str)
        iteration_counter += 1
        if iteration_counter == 10:
            iteration_counter = 0
            print("Map size:", my_map.size())
else:
    client.shutdown()
    raise Exception("Connection failed, check your configuration.")
