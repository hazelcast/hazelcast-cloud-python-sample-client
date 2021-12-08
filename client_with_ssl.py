import hazelcast
import logging
import random
from hazelcast.core import HazelcastJsonValue
from hazelcast.discovery import HazelcastCloudDiscovery
from os.path import abspath

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


def json_serialization_example(hz_client):
    create_mapping_for_countries(hz_client)

    populate_countries_map(hz_client)

    select_all_countries(hz_client)

    create_mapping_for_cities(hz_client)

    populate_city_map(hz_client)

    select_cities_by_country(hz_client, "AU")

    select_countries_and_cities(hz_client)


def create_mapping_for_countries(hz_client):
    # see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
    print("Creating mapping for countries...")

    mapping_query = """
        CREATE OR REPLACE MAPPING country(
            __key VARCHAR, 
            isoCode VARCHAR, 
            country VARCHAR
        )
        TYPE IMap OPTIONS(
            'keyFormat' = 'varchar',
            'valueFormat' = 'json-flat'
        );
    """
    hz_client.sql.execute(mapping_query).result()
    print("Mapping for countries has been created.")
    print("--------------------")


def populate_countries_map(hz_client):
    # see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
    print("Populating 'countries' map with JSON values...")

    au = '{"isoCode": "AU", "country": "Australia"}'
    en = '{"isoCode": "EN", "country": "England"}'
    us = '{"isoCode": "US", "country": "United States"}'
    cz = '{"isoCode": "CZ", "country": "Czech Republic"}'

    countries = hz_client.get_map("country").blocking()
    countries.put("AU", HazelcastJsonValue(au))
    countries.put("EN", HazelcastJsonValue(en))
    countries.put("US", HazelcastJsonValue(us))
    countries.put("CZ", HazelcastJsonValue(cz))
    print("The 'countries' map has been populated.")
    print("--------------------")


def select_all_countries(hz_client):
    query = "SELECT c.country from country c"
    print("Select all countries with sql =", query)

    all_countries_result = hz_client.sql.execute(query).result()
    for row in all_countries_result:
        print("country =", row["country"])
    print("--------------------")


def create_mapping_for_cities(hz_client):
    # see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
    print("Creating mapping for cities...")

    mapping_query = """
        CREATE OR REPLACE MAPPING city(
            __key INT, 
            country VARCHAR, 
            city VARCHAR, 
            population BIGINT
        ) 
        TYPE IMap OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'json-flat'
        );
    """

    hz_client.sql.execute(mapping_query).result()
    print("Mapping for cities has been created.")
    print("--------------------")


def populate_city_map(hz_client):
    # see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
    print("Populating 'city' map with JSON values...")

    cities = hz_client.get_map("city").blocking()
    au = '{"country": "AU", "city": "Canberra", "population": 354644}'
    cz = '{"country": "CZ", "city": "Prague", "population": 1227332}'
    en = '{"country": "EN", "city": "London", "population": 8174100}'
    us = '{"country": "US", "city": "Washington, DC", "population": 601723}'
    cities.put(1, HazelcastJsonValue(au))
    cities.put(2, HazelcastJsonValue(cz))
    cities.put(3, HazelcastJsonValue(en))
    cities.put(4, HazelcastJsonValue(us))
    print("The 'city' map has been populated.")
    print("--------------------")


def select_cities_by_country(hz_client, country):
    query = "SELECT city, population FROM city where country=?"
    print("Select city and population with sql = ", query)

    cities_result = hz_client.sql.execute(query, country).result()
    for row in cities_result:
        print("city = %s, population = %d" % (row["city"], row["population"]))
    print("--------------------")


def select_countries_and_cities(hz_client):
    query = """
        SELECT c.isoCode, c.country, t.city, t.population
        FROM country c
        JOIN city t
        ON c.isoCode = t.country;
    """
    print("Select country and city data in query that joins tables")

    join_result = hz_client.sql.execute(query).result()
    print("%4s | %15s | %20s | %15s |" % ("iso", "country", "city", "population"))
    print("-----------------------------------------------------------------")
    for row in join_result:
        print("%4s | %15s | %20s | %15s |" %
              (row["isoCode"], row["country"], row["city"], row["population"]))
    print("-----------------------------------------------------------------")


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

print("Successfully connected!")

sample_map = client.get_map("map").blocking()

# the 'map_example' is an example with an infinite loop inside, so if you'd like to try other examples,
# don't forget to comment out the following line
map_example(sample_map)

# sql_example(client)

# json_serialization_example(client)

client.shutdown()
