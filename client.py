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
from unicodedata import numeric

import hazelcast
from hazelcast.discovery import HazelcastCloudDiscovery
from hazelcast.serialization.api import (
    CompactSerializer,
    CompactWriter,
    CompactReader,
)

"""
A sample application that configures a client to connect to a Hazelcast Viridian cluster,
and to then insert and fetch data with SQL, thus testing that the connection to
the Hazelcast Viridian cluster is successful.

See: https://docs.hazelcast.com/cloud/get-started
"""


class City:
    def __init__(self, country: str, city: str, population: int) -> None:
        self.country = country
        self.city = city
        self.population = population


class CitySeriazlizer(CompactSerializer[City]):
    def read(self, reader: CompactReader):
        city = reader.read_string("city")
        country = reader.read_string("country")
        population = reader.read_int32("population")
        return City(country=country, city=city, population=population)

    def write(self, writer: CompactWriter, obj: City):
        writer.write_string("country", obj.country)
        writer.write_string("city", obj.city)
        writer.write_int32("population", obj.population)

    def get_type_name(self):
        return "city"

    def get_class(self):
        return City


def create_mapping(client: hazelcast.client):
    print("\nCreating the mapping...")
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


def populate_cities(client: hazelcast.client):
    print("\nInserting data via SQL...")
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
    except hazelcast.sql.HazelcastSqlError as e:
        print(f"FAILED. {e!s}")

    print("\nPutting a city into 'cities' map...")

    # Let's also add a city as object.
    cities = client.get_map("cities").blocking()
    cities.put(8, City("Brazil", "Rio de Janeiro", 13634274))
    print("OK.")


def fetch_cities_via_sql(client: hazelcast.client):
    print("\nFetching cities via SQL...")
    result = client.sql.execute("SELECT __key,this FROM cities").result()
    print("OK.")

    print("\n--Results of 'SELECT __key, this FROM cities'")
    print(
        "| {:4s} | {:20s} | {:20s} | {:15s} |".format(
            "id", "country", "city", "population"
        )
    )

    for row in result:
        id = row["__key"]
        city = row["this"]
        print(
            "| {:4d} | {:20s} | {:20s} | {:15d} |".format(
                id, city.country, city.city, city.population
            )
        )

    print(
        """
    !! Hint !! You can execute your SQL queries on your Viridian cluster over the management center.
    1. Go to 'Management Center' of your Hazelcast Viridian cluster.
    2. Open the 'SQL Browser'.
    3. Try to execute 'SELECT * FROM cities'.
    """
    )


logging.basicConfig(level=logging.INFO)
client = hazelcast.HazelcastClient(
    cluster_name="YOUR_CLUSTER_NAME",
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    statistics_enabled=True,
    compact_serializers=[CitySeriazlizer()],
)
print("Connection Successful!")

create_mapping(client)
populate_cities(client)
fetch_cities_via_sql(client)

client.shutdown()
