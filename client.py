import logging
import random

import hazelcast
from hazelcast.core import HazelcastJsonValue
from hazelcast.discovery import HazelcastCloudDiscovery

"""
This is boilerplate application that configures client to connect Hazelcast Cloud cluster.

See: https://docs.hazelcast.com/cloud/python-client
"""


def city(country: str, name: str, population: int) -> HazelcastJsonValue:
    return HazelcastJsonValue({
        "country": country,
        "city": name,
        "population": population,
    })


def map_example(client: hazelcast.HazelcastClient):
    """
    This example shows how to work with Hazelcast maps.
    """
    cities = client.get_map("cities").blocking()
    cities.put("1", city("United Kingdom", "London", 9_540_576))
    cities.put("2", city("United Kingdom", "Manchester", 2_770_434))
    cities.put("3", city("United States", "New York", 19_223_191))
    cities.put("4", city("United States", "Los Angeles", 3_985_520))
    cities.put("5", city("Turkey", "Ankara", 5_309_690))
    cities.put("6", city("Turkey", "Istanbul", 15_636_243))
    cities.put("7", city("Brazil", "Sao Paulo", 22_429_800))
    cities.put("8", city("Brazil", "Rio de Janeiro", 13_634_274))
    map_size = cities.size()
    print(f"'cities' map now contains {map_size} entries.")
    print("-"*20)


def nonstop_map_example(client: hazelcast.HazelcastClient):
    print("Now the map named 'map' will be filled with random entries.")
    map = client.get_map("map").blocking()
    iteration_counter = 0
    while True:
        random_key = random.randint(0, 99_999)
        map.put(f"key-{random_key}", f"value-{random_key}")
        map.get(f"key-{random.randint(0, 99_999)}")
        iteration_counter += 1
        if iteration_counter == 10:
            iteration_counter = 0
            print(f"Current map size: {map.size()}")


logging.basicConfig(level=logging.INFO)
HazelcastCloudDiscovery._CLOUD_URL_BASE = "YOUR_DISCOVERY_URL"
client = hazelcast.HazelcastClient(
    cluster_name="YOUR_CLUSTER_NAME",
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    statistics_enabled=True,
)
print("Connection Successful!")

map_example(client)
# nonstop_map_example(client)
client.shutdown()
