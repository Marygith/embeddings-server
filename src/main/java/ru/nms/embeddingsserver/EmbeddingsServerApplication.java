package ru.nms.embeddingsserver;


import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.nms.embeddingslibrary.model.MasterServiceMeta;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import static ru.nms.embeddingsserver.util.Constants.PATH_TO_EMBEDDINGS_DIRECTORY;

@SpringBootApplication
public class EmbeddingsServerApplication {

    @Value("${zookeeper.service.base.path}")
    private String basePath;
    private CuratorFramework client;
    private ServiceDiscovery<WorkerServiceMeta> serviceDiscovery;
    @Value("${zookeeper.service.base.max.retry.policy}")
    private int maxRetries;

    @Value("${zookeeper.service.base.sleep.time}")
    private int sleepTimeInMillis;

    @Value("${zookeeper.worker.service.name}")
    private String workerServiceName;

    @Value("${zookeeper.worker.service.instance.name}")
    private String instanceName;
    @Value("${server.port}")
    private int port;

    @Value("${zookeeper.address}")
    private String zookeeperAddress;

    @Value("${zookeeper.worker.service.address}")
    private String address;

    public static void main(String[] args) {
        SpringApplication.run(EmbeddingsServerApplication.class, args);
    }

    @Bean
    public CuratorFramework createClient() {
        return client;
    }

    @Bean
    public ServiceDiscovery<WorkerServiceMeta>  getServiceDiscovery() {
        return serviceDiscovery;
    }

    @PostConstruct
    private void registerServerToZookeeper() {
        try {
            FileUtils.deleteDirectory(new File(PATH_TO_EMBEDDINGS_DIRECTORY + instanceName));
        } catch (IOException e) {
            System.err.println("Did not manage to delete " + PATH_TO_EMBEDDINGS_DIRECTORY + instanceName + " directory");
        }
        client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(sleepTimeInMillis, maxRetries));
        client.start();
        JsonInstanceSerializer<WorkerServiceMeta> serializer = new JsonInstanceSerializer<>(WorkerServiceMeta.class);
        /*ServiceDiscovery<WorkerServiceMeta> */serviceDiscovery = ServiceDiscoveryBuilder.builder(WorkerServiceMeta.class)
                .client(client)
                .serializer(serializer)
                .basePath(basePath)
                .watchInstances(true)
                .build();

        try {
            serviceDiscovery.start();
/*            String id = UUID.randomUUID().toString();
            ServiceInstance<WorkerServiceMeta> instance = ServiceInstance.<WorkerServiceMeta>builder()
                    .id(id)
                    .name(workerServiceName)
                    .port(port)
                    .address(address)   //If address is not written, you will take your local IP.
                    .payload(new WorkerServiceMeta(instanceName, new ArrayList<>(), address, port))
                    .build();
            serviceDiscovery.registerService(instance);

            ServiceProvider<WorkerServiceMeta> serviceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(workerServiceName)
                    .build();
            serviceProvider.start();

            serviceProvider.getAllInstances().forEach(System.out::println);*/
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @PreDestroy
    private void close() {
        client.close();
    }
}
