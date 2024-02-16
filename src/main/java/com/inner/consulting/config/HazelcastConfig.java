package com.inner.consulting.config;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastConfig {

    public static HazelcastInstance hazelcastClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("cluster-prueba");
        return HazelcastClient.newHazelcastClient(clientConfig);
    }
}

