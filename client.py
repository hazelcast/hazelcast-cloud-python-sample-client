import hazelcast
import logging
import random

"""
This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
After successful connection, it puts random entries into the map.

See: https://docs.hazelcast.cloud/docs/python-client
"""
if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()

    # Set up group name and password for authentication
    config.group_config.name = "YOUR_CLUSTER_NAME"
    config.group_config.password = "YOUR_CLUSTER_PASSWORD"

    # Enable Hazelcast.Cloud configuration and set the token of your cluster.
    config.network_config.cloud_config.enabled = True
    config.network_config.cloud_config.discovery_token = "YOUR_CLUSTER_DISCOVERY_TOKEN"
    config.set_property("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL")

    # Start a new Hazelcast client with this configuration.
    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map").blocking()
    my_map.put("key", "value")

    if my_map.get("key") == "value":
        print("Connection Successful!")
        print("Now, `map` will be filled with random entries.");

        iteration_counter = 0
        while True:
            random_key = random.randint(1, 100000)
            my_map.put("key" + str(random_key), "value" + str(random_key))
            my_map.get("key" + str(random.randint(1,100000)))
            iteration_counter += 1
            if iteration_counter == 10:
                iteration_counter = 0
                print("Map size:" + str(my_map.size()))
    else:
        raise Exception("Connection failed, check your configuration.")


    client.shutdown()
