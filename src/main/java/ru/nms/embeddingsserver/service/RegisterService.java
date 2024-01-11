package ru.nms.embeddingsserver.service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RegisterService {

    @Value("${zookeeper.worker.service.name}")
    private String masterServiceName;

    @Value("${zookeeper.worker.service.instance.name}")
    private String instanceName;
    @Value("${server.port}")
    private int port;

    private final CuratorFramework client;

    private final ServiceDiscovery<WorkerServiceMeta> workerServiceDiscovery;

    @Getter
    private ServiceInstance<WorkerServiceMeta> instance;

    @PostConstruct
    private void initInstance() {
        try {
            workerServiceDiscovery.start();
            String id = UUID.randomUUID().toString();
            instance = ServiceInstance.<WorkerServiceMeta>builder()
                    .id(id)
                    .name(masterServiceName)
                    .port(port)
                    .address("localhost")   //If address is not written, you will take your local IP.
                    .payload(new WorkerServiceMeta(instanceName, new ArrayList<>(), "localhost", port))
                    .build();

            workerServiceDiscovery.registerService(instance);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
