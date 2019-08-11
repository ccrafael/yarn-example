package org.rcc.tools.yarn.applicationmaster;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class NodeManagerCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

    private final static Logger logger = LoggerFactory.getLogger(NodeManagerCallbackHandler.class);
    private final ApplicationMaster applicationMaster;


    public NodeManagerCallbackHandler(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
        logger.info("New container added {} ", containerId);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        logger.info("Container stopped [{}].", containerId);
        applicationMaster.removeContainer(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        logger.info("Container status: id={}, status={}.", containerId, containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        logger.info("Container started [{}].", containerId);
    }


    @Override
    public void onStartContainerError(ContainerId containerId, Throwable error) {
        logger.error("Error starting container [{}].", containerId, error);
        applicationMaster.removeContainer(containerId);
    }

    @Override
    public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {
        logger.info("Container [{}] resources increased  with [{}]. ", containerId, resource);
    }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
        logger.info("Container [{}] resource updated  with [{}]. ", containerId, resource);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable error) {
        logger.error("Failed to get container [{}]. ", containerId, error);
    }

    @Override
    public void onIncreaseContainerResourceError(ContainerId containerId, Throwable error) {
        logger.error("Container [{}] failed to increase resources.", containerId, error);
    }

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId, Throwable error) {
        logger.error("Container [{}] failed to update resources.", containerId, error);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        logger.error("Failed to stop container [{}].", containerId);
        applicationMaster.removeContainer(containerId);
    }

}
