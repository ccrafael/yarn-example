package org.rcc.tools.yarn.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Properties;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(Util.class.getResourceAsStream("/application.properties"));
        try {
            properties.load(Files.newInputStream(Paths.get("application.properties")));
        } catch (NoSuchFileException e) {
            logger.warn("No application.properties provided. The internal application.properties will be used.");
        }
        return properties;
    }
}
