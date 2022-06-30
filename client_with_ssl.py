import hazelcast
import logging
import random
from hazelcast.discovery import HazelcastCloudDiscovery
from os.path import abspath

"""
This is a boilerplate client application that connects to your Hazelcast Viridian cluster.
See: https://docs.hazelcast.com/cloud/get-started

Snippets of this code are included as examples in our documentation,
using the tag:: comments.
"""

logging.basicConfig(level=logging.INFO)
# tag::env[]
# Define which environment to use such as production, uat, or dev
HazelcastCloudDiscovery._CLOUD_URL_BASE = "YOUR_DISCOVERY_URL"
# end::env[]
# Configure the client to connect to the cluster
# tag::config[]
client = hazelcast.HazelcastClient(
    cluster_name="YOUR_CLUSTER_NAME",
    # The cluster discovery token is a unique token that maps to the current IP address of the cluster.
	# Cluster IP addresses may change.
	# This token allows clients to find out the current IP address
	# of the cluster and connect to it.
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    # Allow the client to collect metrics
	# so that you can see client statistics in Management Center.
	# See https://pkg.go.dev/github.com/hazelcast/hazelcast-go-client#hdr-Management_Center_Integration
    statistics_enabled=True,
    # Configure TLS
	# When you download the sample client from the console,
	# the files are downloaded along with this client code.
    ssl_enabled=True,
    ssl_cafile=abspath("ca.pem"),
    ssl_certfile=abspath("cert.pem"),
    ssl_keyfile=abspath("key.pem"),
    ssl_password="YOUR_SSL_PASSWORD",
)
# end::config[]

my_map = client.get_map("map").blocking()
my_map.put("key", "value")

if my_map.get("key") == "value":
    print("Successfully connected!")
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
else:
    client.shutdown()
    raise Exception("Connection failed, check your configuration.")
