package org.pih.petl;

import com.github.dockerjava.api.model.Container;

import java.util.Arrays;

public class DockerConnectorTest {

    public void testIsRunning() {
        try (DockerConnector docker = DockerConnector.open()) {
            for (Container container : docker.getContainers()) {
                System.out.println("Container: " + Arrays.asList(container.getNames()));
                System.out.println("Is running: " + docker.isContainerRunning(container));
            }
        }
    }

}
