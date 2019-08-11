package org.rcc.tools.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.rcc.tools.yarn.resources.LocalResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_COMMAND;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_MEMORY;
import static org.rcc.tools.yarn.util.Constants.APPLICATIONMASTER_VCPU;
import static org.rcc.tools.yarn.util.Constants.APPLICATION_NAME;
import static org.rcc.tools.yarn.util.Constants.APPLICATION_QUEUE;
import static org.rcc.tools.yarn.util.Constants.FS_DEFAULT_FS;
import static org.rcc.tools.yarn.util.Constants.LAUNCHER_SLEEP_WAIT;
import static org.rcc.tools.yarn.util.Constants.LAUNCHER_WAIT;
import static org.rcc.tools.yarn.util.Constants.MAPREDUCE_FRAMEWORK_NAME;
import static org.rcc.tools.yarn.util.Constants.YARN_RESOURCEMANAGER_ADDRESS;
import static org.rcc.tools.yarn.util.Constants.YARN_RESOURCEMANAGER_HOSTNAME;
import static org.rcc.tools.yarn.util.Util.loadProperties;

public class Launcher {
    private final static Logger logger = LoggerFactory.getLogger(Launcher.class);

    private final Configuration conf;
    private YarnClient yarnClient;
    private YarnClientApplication application;
    private ApplicationId appId;

    private final Map<String, String> env;
    private final Properties properties;

    private ApplicationSubmissionContext appContext;
    private LocalResourceManager localResources;


    public Launcher() throws IOException {

        properties = loadProperties();
        conf = new YarnConfiguration();
        env = new HashMap<>();

        setClusterConf(FS_DEFAULT_FS);
        setClusterConf(YARN_RESOURCEMANAGER_HOSTNAME);
        setClusterConf(YARN_RESOURCEMANAGER_ADDRESS);
        setClusterConf(MAPREDUCE_FRAMEWORK_NAME);
    }

    private void setClusterConf(String property) {
        String value = properties.getProperty(property);
        if (value != null && !value.trim().isEmpty()) {
            conf.set(property, value);
        }
    }

    public void run(String jarPath) throws IOException, YarnException, InterruptedException {

        startYarnClient();

        buildAppContext(jarPath);
        yarnClient.submitApplication(appContext);

        if ("true".equals(properties.getProperty(LAUNCHER_WAIT))) {
            waitForEnd();
        }

        yarnClient.stop();
        yarnClient.close();
    }

    private void startYarnClient() {
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(this.conf);
        this.yarnClient.start();

        logger.info("Yarn client started.");
    }

    private void buildAppContext(String jarPath) throws IOException, YarnException {
        String appName = properties.getProperty(APPLICATION_NAME);

        application = yarnClient.createApplication();
        appContext = application.getApplicationSubmissionContext();
        appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(true);
        appContext.setApplicationName(appName);

        localResources = new LocalResourceManager(conf, appName, appId.toString());

        buildEnvironment();

        uploadApplicationJar(jarPath);
        uploadApplicationPropertiesOverride();

        List<String> commands = createAppMasterCommand();

        Resource resources = Resource.newInstance(getInt(APPLICATIONMASTER_MEMORY), getInt(APPLICATIONMASTER_VCPU));
        appContext.setResource(resources);

        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources.getLocalResources(), env, commands, null, null, null);

        appContext.setAMContainerSpec(amContainer);
        appContext.setPriority(Priority.UNDEFINED);
        appContext.setQueue(properties.getProperty(APPLICATION_QUEUE));
    }

    private int getInt(String property) {
        String value = properties.getProperty(property);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Wrong value for " + property + " = [" + value + "].");
        }
    }

    private void buildEnvironment() {
        env.put("HADOOP_USER_NAME", properties.getProperty("hdfs.user.name"));
    }

    private void uploadApplicationJar(String jarPath) throws IOException {
        localResources.uploadResource(new Path(jarPath), "app.jar");
        localResources.addLocalFileResource("app.jar",
                localResources.getResourcePath("app.jar"));

    }

    private void uploadApplicationPropertiesOverride() throws IOException {
        java.nio.file.Path overridePropertiesPath = Paths.get("application.properties");
        if (Files.exists(overridePropertiesPath)) {
            localResources.uploadResource(new Path(overridePropertiesPath.toUri()), "application.properties");
            localResources.addLocalFileResource("application.properties",
                    localResources.getResourcePath("application.properties"));
        }

    }

    private void waitForEnd() throws IOException, YarnException, InterruptedException {
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();

        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            logger.info(" Application state {}. ", appState);
            Thread.sleep(getInt(LAUNCHER_SLEEP_WAIT));
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        logger.info(" Application state {} at {}.", appState, new Date(appReport.getFinishTime()));
    }

    private List<String> createAppMasterCommand() {

        List<String> vargs = new ArrayList<>();

        vargs.add(properties.getProperty(APPLICATIONMASTER_COMMAND));
        vargs.add("1>" + LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        return Arrays.asList(String.join(" ", vargs));
    }


    public static void main(String[] args) throws Exception {
        Launcher c = new Launcher();
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: mergeTool <jarpath>");
        }
        c.run(args[0]);
    }

}
