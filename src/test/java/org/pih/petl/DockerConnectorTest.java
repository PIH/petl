package org.pih.petl;

import com.github.dockerjava.api.model.Container;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

import java.util.Arrays;

@Disabled
public class DockerConnectorTest {

    @Test
    public void testIsRunning() {
        try (DockerConnector docker = DockerConnector.open()) {
            for (Container container : docker.getContainers()) {
                System.out.println("Container: " + Arrays.asList(container.getNames()));
                System.out.println("Is running: " + docker.isContainerRunning(container));
            }
        }
    }

}
