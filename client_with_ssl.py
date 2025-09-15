"""
 Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 
 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

import logging
import os
import typing

import hazelcast
from hazelcast import HazelcastClient
from hazelcast.discovery import HazelcastCloudDiscovery
from hazelcast.serialization.api import CompactReader, CompactSerializer, CompactWriter

"""
A sample application that configures a client to connect to a Hazelcast Cloud cluster
over TLS, and to then insert and fetch data with SQL, thus testing that the connection to
the Hazelcast Cloud cluster is successful.

See: https://docs.hazelcast.com/cloud/get-started
"""


class City:
    def __init__(self, country: str, city: str, population: int) -> None:
        self.country = country
        self.city = city
        self.population = population


class CitySerializer(CompactSerializer[City]):
    def read(self, reader: CompactReader) -> City:
        city = reader.read_string("city")
        country = reader.read_string("country")
        population = reader.read_int32("population")
        return City(country, city, population)

    def write(self, writer: CompactWriter, obj: City) -> None:
        writer.write_string("country", obj.country)
        writer.write_string("city", obj.city)
        writer.write_int32("population", obj.population)

    def get_type_name(self) -> str:
        return "city"

    def get_class(self) -> typing.Type[City]:
        return City


def create_mapping(client: HazelcastClient) -> None:
    print("Creating the mapping...", end="")
    # See: https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps
    mapping_query = """
        CREATE OR REPLACE MAPPING 
            cities (
                __key INT,                                        
                country VARCHAR,
                city VARCHAR,
                population INT) TYPE IMAP
            OPTIONS ( 
                'keyFormat' = 'int',
                'valueFormat' = 'compact',
                'valueCompactTypeName' = 'city')
    """
    client.sql.execute(mapping_query).result()
    print("OK.")


def populate_cities(client: HazelcastClient) -> None:
    print("Cleaning up the 'cities' map...", end="")
    try:
        client.sql.execute("DELETE FROM cities").result()
        print("OK.")
    except Exception as e:
        print(f"FAILED: {e!s}.")

    print("Inserting data via SQL...", end="")

    insert_query = """
        INSERT INTO cities 
        (__key, city, country, population) VALUES
        (1, 'London', 'United Kingdom', 9540576),
        (2, 'Manchester', 'United Kingdom', 2770434),
        (3, 'New York', 'United States', 19223191),
        (4, 'Los Angeles', 'United States', 3985520),
        (5, 'Istanbul', 'Türkiye', 15636243),
        (6, 'Ankara', 'Türkiye', 5309690),
        (7, 'Sao Paulo ', 'Brazil', 22429800)
    """

    try:
        client.sql.execute(insert_query).result()
        print("OK.")
    except Exception as e:
        print(f"FAILED: {e!s}.")

    print("Putting a city into 'cities' map...", end="")

    # Let's also add a city as object.
    cities = client.get_map("cities").blocking()
    cities.put(8, City("Brazil", "Rio de Janeiro", 13634274))
    print("OK.")


def fetch_cities_via_sql(client: HazelcastClient) -> None:
    print("Fetching cities via SQL...", end="")
    result = client.sql.execute("SELECT __key, this FROM cities").result()
    print("OK.")

    print("--Results of 'SELECT __key, this FROM cities'")
    print(f"| {'id':>4} | {'country':<20} | {'city':<20} | {'population':<15} |")

    for row in result:
        city = row["this"]
        print(
            f"| {row['__key']:>4} | {city.country:<20} | {city.city:<20} | {city.population:<15} |"
        )

    print(
        "\n!! Hint !! You can execute your SQL queries on your Hazelcast Cloud cluster using the 'SQL Broswer' UI.",
        "1. Start one of the preloaded demos in your Trial Experience.",
        "2. This will open the 'SQL Browser'.",
        "3. Add a new Tab.",
        "4. Try to execute 'SELECT * FROM cities'.",
        sep="\n",
    )


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
    compact_serializers=[CitySerializer()],
)

print("Connection Successful!")

create_mapping(client)
populate_cities(client)
fetch_cities_via_sql(client)

client.shutdown()
