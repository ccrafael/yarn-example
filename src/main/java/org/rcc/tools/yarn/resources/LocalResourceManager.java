package org.rcc.tools.yarn.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalResourceManager {
    private final static Logger logger = LoggerFactory.getLogger(LocalResourceManager.class);
    private static final String BACKLASH = "/";
    private static final short FS_PERMISSION = 0710;

    private final FileSystem fileSystem;
    private final Map<String, LocalResource> localResources;
    private final String appName;
    private final String appId;

    public LocalResourceManager(Configuration conf, String appName, String appId) throws IOException {
        this.fileSystem = FileSystem.get(conf);
        localResources = new HashMap<>();
        this.appName = appName;
        this.appId = appId;
    }

    public Path getResourcePath(String name) {
        return getResourcesHome(fileSystem).suffix(BACKLASH+name);
    }

    public void uploadResource(Path sourcePath, String name) throws IOException {

        Path destPath = getResourcePath(name);
        logger.info("Upload {} to {}.", sourcePath, destPath);
        fileSystem.copyFromLocalFile(sourcePath, destPath);
    }

    private Path getResourcesHome(FileSystem fileSystem) {
        String suffix = appName + BACKLASH + appId + BACKLASH;
        Path path = new Path(fileSystem.getHomeDirectory(), suffix);
        return path;
    }

    public void addLocalFileResource(String name, Path resourcePath) throws IOException {

        logger.info("Add local resource name [{}] path [{}].", name, resourcePath);
        FileStatus scFileStatus = fileSystem.getFileStatus(resourcePath);

        LocalResource localResource = LocalResource.newInstance(
                URL.fromURI(resourcePath.toUri()),
                LocalResourceType.FILE,
                LocalResourceVisibility.PUBLIC,
                scFileStatus.getLen(),
                scFileStatus.getModificationTime());


        localResources.put(name, localResource);
    }

    public Map<String, LocalResource> getLocalResources() {
        return localResources;
    }
}