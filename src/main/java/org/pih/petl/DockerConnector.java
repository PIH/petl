/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.pih.petl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Utility methods useful for manipulating SQL statements
 */
public class DockerConnector implements Closeable {

    private final DockerClientConfig dockerClientConfig;
    private final DockerHttpClient dockerHttpClient;
    private final DockerClient dockerClient;

    private DockerConnector() {
        dockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        dockerHttpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(dockerClientConfig.getDockerHost())
                .sslConfig(dockerClientConfig.getSSLConfig())
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build();
        dockerClient = DockerClientImpl.getInstance(dockerClientConfig, dockerHttpClient);
    }

    public static DockerConnector open() {
        return new DockerConnector();
    }

    public void close() {
        IOUtils.closeQuietly(dockerClient);
    }

    public List<Container> getContainers() {
        return dockerClient.listContainersCmd().withShowAll(true).exec();
    }

    public Container getContainer(String containerName) {
        for (Container container : getContainers()) {
            List<String> names = Arrays.asList(container.getNames());
            if (names.contains(containerName) || names.contains("/" + containerName)) {
                return container;
            }
        }
        return null;
    }

    public boolean containerExists(String containerName) {
        return getContainer(containerName) != null;
    }

    public boolean isContainerRunning(Container container) {
        return "running".equalsIgnoreCase(container.getState());
    }

    public void startContainer(Container container) {
        dockerClient.startContainerCmd(container.getId()).exec();
    }

    public void stopContainer(Container container) {
        dockerClient.stopContainerCmd(container.getId()).exec();
    }
}
