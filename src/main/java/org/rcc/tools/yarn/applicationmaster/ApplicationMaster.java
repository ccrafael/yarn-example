package org.rcc.tools.yarn.applicationmaster;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.rcc.tools.yarn.resources.LocalResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import static java.lang.Integer.min;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.CONTAINER_ID;
import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;
import static org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.createAMRMClientAsync;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_NUMCONTAINERS;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_WORKER_COMMAND;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_WORKER_MEMORY;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_WORKER_VCPU;
import static org.rcc.tools.yarn.util.Constants.APPLICATION_NAME;
import static org.rcc.tools.yarn.util.Util.loadProperties;

public class ApplicationMaster {
    private final static Logger logger = LoggerFactory.getLogger(ApplicationMaster.class);

    private final YarnConfiguration conf;
    private final Properties properties;

    private AMRMClientAsync<ContainerRequest> resourceManagerClient;
    private ResourceManagerCallbackHandler allocListener;
    private NodeManagerCallbackHandler containerListener;
    private NMClientAsyncImpl nodeManagerClient;

    private LocalResourceManager localResources;
    private Map<String, String> shellEnv;

    private int containerMemory;
    private int containerVirtualCores;
    private ApplicationId appId;

    private final Map<ContainerId, Container> containers;


    public ApplicationMaster() throws IOException {
        conf = new YarnConfiguration();
        containers = new HashMap<>();

        properties = loadProperties();
    }

    private void run() throws IOException, YarnException {

        loadApplicationMasterInfo();

        connectWithResourceManager();
        connectWithNodeManagers();

        RegisterApplicationMasterResponse response = registerSelfWithApplicationMaster();

        configureResources(response);

        requestContainers(response);

    }

    private void requestContainers(RegisterApplicationMasterResponse response) {
        List<Container> previousAttemptsContainers = response.getContainersFromPreviousAttempts();

        logger.info("Received {} previous containers running on previous attempt.", previousAttemptsContainers);
        int numTotalContainersToRequest = getInt(APPLICATIONMASTER_NUMCONTAINERS) - previousAttemptsContainers.size();

        IntStream.range(0, numTotalContainersToRequest)
                .mapToObj(i -> setupContainerAskForRM())
                .forEach(resourceManagerClient::addContainerRequest);
    }

    public void startContainer(Container container) throws IOException {

        List<String> commands = getCommandToLaunchOnContainer();

        localResources = new LocalResourceManager(conf, properties.getProperty(APPLICATION_NAME), appId.toString());

        localResources.addLocalFileResource("app.jar", localResources.getResourcePath("app.jar"));

        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources.getLocalResources(),
                shellEnv,
                commands,
                null,
                null,
                null);


        addContainer(container.getId(), container);
        nodeManagerClient.startContainerAsync(container, ctx);

    }

    private void addContainer(ContainerId id, Container container) {
        this.containers.put(id, container);
    }

    private void loadApplicationMasterInfo() {
        Map<String, String> envs = System.getenv();

        logger.info("Aplication Master env: \n {}", envs);

        String containerIdString = envs.get(CONTAINER_ID.key());

        logger.info("Application running into {}.", containerIdString);

        if (containerIdString == null) {
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }

        ContainerId containerId = ContainerId.fromString(containerIdString);

        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        appId = appAttemptID.getApplicationId();

        logger.info("Container: \n {}.\nApplication attempt: \n {}.", containerId, appAttemptID);

    }

    public void unregisterApplication(FinalApplicationStatus appStatus, String appMessage) {
        try {
            resourceManagerClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException | IOException ex) {
            logger.error("Failed to unregister application", ex);
        } finally {
            stop();
            close();
        }
    }

    private RegisterApplicationMasterResponse registerSelfWithApplicationMaster() throws YarnException, IOException {
        String appMasterHostname = NetUtils.getHostname();

        return resourceManagerClient
                .registerApplicationMaster(appMasterHostname, 0, "");
    }

    private List<String> getCommandToLaunchOnContainer() {

        List<String> vargs = new ArrayList<>();

        vargs.add(properties.getProperty(APPLICATIONMASTER_WORKER_COMMAND));
        vargs.add("1>" + LOG_DIR_EXPANSION_VAR + "/Worker.stdout");
        vargs.add("2>" + LOG_DIR_EXPANSION_VAR + "/Worker.stderr");

        return Arrays.asList(String.join(" ", vargs));
    }

    private void connectWithNodeManagers() {
        containerListener = new NodeManagerCallbackHandler(this);
        nodeManagerClient = new NMClientAsyncImpl(containerListener);
        nodeManagerClient.init(conf);
        nodeManagerClient.start();
    }

    private void connectWithResourceManager() {
        allocListener = new ResourceManagerCallbackHandler(this);
        resourceManagerClient = createAMRMClientAsync(1000, allocListener);
        resourceManagerClient.init(conf);
        resourceManagerClient.start();

    }

    private void stop() {
        nodeManagerClient.stop();
        resourceManagerClient.stop();
    }

    private void close() {
        try {
            nodeManagerClient.close();
        } catch (IOException e) {
            logger.error("Error closing connection with node manger", e);
        }

        try {
            resourceManagerClient.close();
        } catch (IOException e) {
            logger.error("Error closing connection with resource manager", e);
        }
    }

    private void configureResources(RegisterApplicationMasterResponse response) {
        int maxMem = response.getMaximumResourceCapability().getMemory();
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();

        logger.info("Cluster info \n\t- mem {}mb. \n\t- vCores {}.\n" +
                        "Desired resources to request \n\t- mem {}mb. \n\t- vCores {}.",
                maxMem,
                maxVCores,
                getInt(APPLICATIONMASTER_WORKER_MEMORY),
                getInt(APPLICATIONMASTER_WORKER_VCPU));

        containerMemory = min(getInt(APPLICATIONMASTER_WORKER_MEMORY), maxMem);
        containerVirtualCores = min(getInt(APPLICATIONMASTER_WORKER_VCPU), maxVCores);

        logger.info("Final resources to request \n\t- mem {}mb. \n\t- vCores {}.",
                containerMemory,
                containerVirtualCores);
    }

    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Priority.UNDEFINED;

        Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);

        logger.info("Request container {}mb {}cores.", containerMemory, containerVirtualCores);

        AMRMClient.ContainerRequest request = new ContainerRequest(capability,
                null, null, pri);

        logger.info("Requested container ask: [{}] ", request);

        return request;
    }

    public void completed(ContainerId containerId) {
        this.containers.remove(containerId);
        if (this.containers.isEmpty()) {
            this.success();
        }
    }

    private void success() {
        this.unregisterApplication(FinalApplicationStatus.SUCCEEDED, "Finish");
    }

    public void removeContainer(ContainerId containerId) {
        this.containers.remove(containerId);
    }

    private int getInt(String property) {
        String value = properties.getProperty(property);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Wrong value for " + property + " = [" + value + "].");
        }
    }

    public static void main(String[] args) throws IOException, YarnException {
        ApplicationMaster applicationMaster = new ApplicationMaster();
        applicationMaster.run();
    }
}
