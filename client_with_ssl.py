import logging
import random
import os

import hazelcast
from hazelcast.core import HazelcastJsonValue
from hazelcast.discovery import HazelcastCloudDiscovery

"""
This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
After successful connection, it puts random entries into the map.

See: https://docs.hazelcast.com/cloud/python-client
"""


def city(country: str, name: str, population: int) -> HazelcastJsonValue:
    return HazelcastJsonValue({
        "country": country,
        "city": name,
        "population": population,
    })


def country(code: str, name: str) -> HazelcastJsonValue:
    return HazelcastJsonValue({
        "isoCode": code,
        "country": name,
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


def sql_example(client: hazelcast.HazelcastClient):
    svc = client.sql
    create_mapping_for_capitals(svc)
    clear_capitals(svc)
    populate_capitals(svc)
    select_all_capitals(svc)
    select_capital_names(svc)


def create_mapping_for_capitals(sql_service: hazelcast.client.SqlService):
    print("Creating a mapping...")
    # See: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps
    mappingQuery = '''
    CREATE OR REPLACE MAPPING capitals
        TYPE IMap
        OPTIONS (
            'keyFormat' = 'varchar',
            'valueFormat' = 'varchar'
        )
    '''
    sql_service.execute(mappingQuery).result()
    print("-" * 20)


def clear_capitals(sql_service: hazelcast.client.SqlService):
    print("Deleting data via SQL...")
    sql_service.execute("DELETE FROM capitals").result()
    print("The data has been deleted successfully.")
    print("-" * 20)


def populate_capitals(sql_service: hazelcast.client.SqlService):
    print("Inserting data via SQL...")
    insertQuery = '''
        INSERT INTO capitals VALUES
            ('Australia','Canberra'),
            ('Croatia','Zagreb'),
            ('Czech Republic','Prague'),
            ('England','London'),
            ('Turkey','Ankara'),
            ('United States','Washington, DC');
    '''
    sql_service.execute(insertQuery).result()
    print("-" * 20)


def select_all_capitals(sql_service: hazelcast.client.SqlService):
    print("Retrieving all the data via SQL...")
    result = sql_service.execute("SELECT * FROM capitals").result()
    for row in result:
        country = row.get_object_with_index(0)
        city = row.get_object_with_index(1)
        print(f"{country} - {city}")
    print("-" * 20)


def select_capital_names(sql_service: hazelcast.client.SqlService):
    print("Retrieving the capital name via SQL...")
    result = sql_service.execute("SELECT __key, this FROM capitals WHERE __key = ?", "United States").result()
    for row in result:
        country = row.get_object("__key")
        city = row.get_object("__key")
        print(f"Country name: {country}; Capital name: {city}")
    print("-" * 20)


def json_serialization_example(client):
    svc = client.sql
    create_mapping_for_countries(svc)
    populate_countries_map(client)
    select_all_countries(svc)
    create_mapping_for_cities(svc)
    populate_cities(client)
    select_cities_by_country(svc, "AU")
    select_countries_and_cities(svc)


def create_mapping_for_countries(sql_service: hazelcast.client.SqlService):
    # see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
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
    sql_service.execute(mapping_query).result()
    print("Mapping for countries has been created.")
    print("-" * 20)


def populate_countries_map(client: hazelcast.HazelcastClient):
    # see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
    print("Populating 'countries' map with JSON values...")
    countries = client.get_map("country").blocking()
    countries.put("AU", country("AU", "Australia"))
    countries.put("EN", country("EN", "England"))
    countries.put("US", country("US", "United States"))
    countries.put("CZ", country("CZ", "Czech Republic"))
    print("The 'countries' map has been populated.")
    print("-" * 20)


def select_all_countries(sql_service: hazelcast.client.SqlService):
    sql = "SELECT c.country from country c"
    print(f"Select all countries with sql = {sql}", sql)
    result = sql_service.execute(sql).result()
    for row in result:
        print(f"country = {row['country']}")
    print("-" * 20)


def create_mapping_for_cities(sql_service: hazelcast.client.SqlService):
    # see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
    print("Creating mapping for cities...")
    mapping_sql = """
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

    sql_service.execute(mapping_sql).result()
    print("Mapping for cities has been created.")
    print("-" * 20)


def populate_cities(client: hazelcast.HazelcastClient):
    # see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
    print("Populating 'city' map with JSON values...")
    cities = client.get_map("city").blocking()
    cities.put(1, city("AU", "Canberra", 467_194))
    cities.put(2, city("CZ", "Prague", 1_318_085))
    cities.put(3, city("EN", "London", 9_540_576))
    cities.put(4, city("US", "Washington, DC", 7_887_965))
    print("The 'city' map has been populated.")
    print("-"*20)


def select_cities_by_country(sql_service: hazelcast.client.SqlService, country: str):
    sql = "SELECT city, population FROM city where country=?"
    print("-" * 20)
    print(f"Select city and population with sql = {sql}")
    result = sql_service.execute(sql, country).result()
    for row in result:
        print(f"city = {row['city']}, population = {row['population']}")
    print("-" * 20)


def select_countries_and_cities(sql_service: hazelcast.client.SqlService):
    sql = """
        SELECT c.isoCode, c.country, t.city, t.population
        FROM country c
        JOIN city t
        ON c.isoCode = t.country;
    """
    print("Select country and city data in query that joins tables")
    print("%4s | %15s | %20s | %15s |" % ("iso", "country", "city", "population"))
    result = sql_service.execute(sql).result()
    for row in result:
        print("%4s | %15s | %20s | %15s |" %
              (row["isoCode"], row["country"], row["city"], row["population"]))
    print("-" * 20)


def nonstop_map_example(client: hazelcast.HazelcastClient):
    print("Now the map named 'map' will be filled with random entries.")
    map = client.get_map("map").blocking()
    iterationCounter = 0
    while True:
        randomKey = random.randint(0, 99_999)
        map.put(f"key-{randomKey}", f"value-{randomKey}")
        map.get(f"key-{random.randint(0, 99_999)}")
        iterationCounter += 1
        if iterationCounter == 10:
            iterationCounter = 0
            print(f"Current map size: {map.size()}")


logging.basicConfig(level=logging.INFO)
HazelcastCloudDiscovery._CLOUD_URL_BASE = "YOUR_DISCOVERY_URL"
client = hazelcast.HazelcastClient(
    cluster_name="YOUR_CLUSTER_NAME",
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    statistics_enabled=True,
    ssl_enabled=True,
    ssl_cafile=os.path.abspath("ca.pem"),
    ssl_certfile=os.path.abspath("cert.pem"),
    ssl_keyfile=os.path.abspath("key.pem"),
    ssl_password="YOUR_SSL_PASSWORD",
)
print("Connection Successful!")

map_example(client)
# sql_example(client)
# json_serialization_example(client)
# nonstop_map_example(client)
client.shutdown()
