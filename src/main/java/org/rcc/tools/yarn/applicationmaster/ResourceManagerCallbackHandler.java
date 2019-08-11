package org.rcc.tools.yarn.applicationmaster;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ResourceManagerCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {

    private final static Logger logger = LoggerFactory.getLogger(ResourceManagerCallbackHandler.class);
    private final ApplicationMaster aplicationMaster;

    public ResourceManagerCallbackHandler(ApplicationMaster aplicationMaster) {
        this.aplicationMaster = aplicationMaster;
    }
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        logger.info("Container complete: [{}].", statuses);
        for (ContainerStatus status : statuses) {

            if (status.getState() == ContainerState.COMPLETE) {
                this.aplicationMaster.completed(status.getContainerId());
            }
        }

    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        logger.info("Container allocated [{}].", containers);
        for (Container container : containers) {
            try {
                this.aplicationMaster.startContainer(container);
            } catch (IOException e) {
                logger.error("Error creating container.", e);
            }
        }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {
        logger.info("Container updated [{}].", containers);
    }

    @Override
    public void onShutdownRequest() {
        logger.info("Shutdown requested.");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        logger.info("Nodes updated [{}].", updatedNodes);
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable error) {
        logger.error("Unknown error.", error);
    }


}
