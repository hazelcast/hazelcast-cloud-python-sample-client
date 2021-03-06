import hazelcast
import logging
import random
from hazelcast.discovery import HazelcastCloudDiscovery

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
)

my_map = client.get_map("map").blocking()
my_map.put("key", "value")

if my_map.get("key") == "value":
    print("Successfully connected!")
    print("Now, 'map' will be filled with random entries.")
    while True:
        random_key = random.randint(1, 100000)
        random_key_str = str(random_key)
        my_map.put("key" + random_key_str, "value" + random_key_str)
        my_map.get("key" + random_key_str)
        if random_key % 10 == 0:
            print("Map size:", my_map.size())
else:
    client.shutdown()
    raise Exception("Connection failed, check your configuration.")
