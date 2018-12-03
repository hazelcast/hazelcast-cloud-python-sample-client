import hazelcast
import logging
import os

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()

    # Set up group name and password for authentication
    config.group_config.name = "YOUR_CLUSTER_NAME"
    config.group_config.password = "YOUR_CLUSTER_PASSWORD"

    # Enable SSL for encryption. Default CA certificates will be used.
    config.network_config.ssl_config.enabled = True

    # Enable Hazelcast.Cloud configuration and set the token of your cluster.
    config.network_config.cloud_config.enabled = True
    config.network_config.cloud_config.discovery_token = "YOUR_CLUSTER_DISCOVERY_TOKEN"

    # Start a new Hazelcast client with this configuration.
    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map-on-the-cloud")
    my_map.put("key", "hazelcast.cloud")

    print(my_map.get("key"))

    client.shutdown()
