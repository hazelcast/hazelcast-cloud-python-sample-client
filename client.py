import hazelcast
import logging
import random
from hazelcast.discovery import HazelcastCloudDiscovery

"""
This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
After successful connection, it puts random entries into the map.

See: https://docs.hazelcast.cloud/docs/python-client
"""


def map_example(my_map):
    print("Now, 'map' will be filled with random entries.")

    iteration_counter = 0
    while True:
        random_key = str(random.randint(1, 100000))
        my_map.put("key" + random_key, "value" + random_key)
        my_map.get("key" + random_key)
        iteration_counter += 1
        if iteration_counter == 10:
            iteration_counter = 0
            print("Map size:", my_map.size())


def sql_example(hz_client):
    print("Creating a mapping...")
    # See: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps
    mapping_query = "CREATE OR REPLACE MAPPING cities TYPE IMap " \
                    "OPTIONS ('keyFormat'='varchar','valueFormat'='varchar')"
    hz_client.sql.execute(mapping_query).result()
    print("The mapping has been created successfully.")
    print("--------------------")

    print("Deleting data via SQL...")
    delete_query = "DELETE FROM cities"
    hz_client.sql.execute(delete_query).result()
    print("The data has been deleted successfully.")
    print("--------------------")

    print("Inserting data via SQL...")
    insert_query = """
    INSERT INTO cities VALUES
    ('Australia','Canberra'),
    ('Croatia','Zagreb'),
    ('Czech Republic','Prague'),
    ('England','London'),
    ('Turkey','Ankara'),
    ('United States','Washington, DC');
    """
    hz_client.sql.execute(insert_query).result()
    print("The data has been inserted successfully.")
    print("--------------------")

    print("Retrieving all the data via SQL...")
    result = hz_client.sql.execute("SELECT * FROM cities").result()
    for row in result:
        country = row[0]
        city = row[1]
        print("%s - %s" % (country, city))
    print("--------------------")

    print("Retrieving a city name via SQL...")
    result = hz_client.sql.execute("SELECT __key, this FROM cities WHERE __key = ?", "United States").result()
    for row in result:
        country = row["__key"]
        city = row["this"]
        print("Country name: %s; City name: %s" % (country, city))
        print("--------------------")
        exit(0)


logging.basicConfig(level=logging.INFO)
HazelcastCloudDiscovery._CLOUD_URL_BASE = "YOUR_DISCOVERY_URL"
client = hazelcast.HazelcastClient(
    cluster_name="YOUR_CLUSTER_NAME",
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    statistics_enabled=True,
)

print("Successfully connected!")

sample_map = client.get_map("map").blocking()

# the 'map_example' is an example with an infinite loop inside, so if you'd like to try other examples,
# don't forget to comment out the following line
map_example(sample_map)

# sql_example(client)

client.shutdown()
