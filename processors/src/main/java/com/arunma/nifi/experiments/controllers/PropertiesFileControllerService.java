package com.arunma.nifi.experiments.controllers;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"properties"})
@CapabilityDescription("Provides a controller service to load config from property files.")
public class PropertiesFileControllerService extends AbstractControllerService implements PropertiesFileService {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesFileControllerService.class);

    public static final PropertyDescriptor PROPERTY_FILE_LOCATION = new PropertyDescriptor.Builder()
            .name("Property File Location")
            .description("Location of the property file to be loaded into configuration")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Cache Service")
            .description("Distributed Map Cache Client Service")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private Properties properties = new Properties();
    private Serializer<String> stringSerializer = new Serializer<String>() {
        @Override
        public void serialize(String value, OutputStream output) throws SerializationException, IOException {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    };

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROPERTY_FILE_LOCATION);
        //props.add(CACHE_SERVICE);
        propertyDescriptors = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public Map<String, String> getAllProperties() {
        Map<String, String> copyOfProperties = new HashMap<>();
        for (String property : properties.stringPropertyNames()) {
            copyOfProperties.put(property, properties.getProperty(property));
        }
        return copyOfProperties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws IOException {
        logger.info("Loading properties file");

        DistributedMapCacheClient cacheService = context.getProperty(CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        String propertyUrl = context.getProperty(PROPERTY_FILE_LOCATION).getValue();

        properties = loadProperties(propertyUrl);

        logger.info("Pushing Properties into Cache service");
        for (String property : properties.stringPropertyNames()) {
            logger.info("Proper Name {} Value {}", property, properties.getProperty(property));
            cacheService.put(property, properties.getProperty(property), stringSerializer, stringSerializer);
        }

    }

    private Properties loadProperties(String propertyUrl) throws IOException {

        File propFile = new File(propertyUrl);
        Properties props = new Properties();
        if (propFile.isFile()) {
            props.load(new FileInputStream(propFile));
        } else {
            logger.error("Property file location configured is not a file. (Directories aren't supported)");
        }
        return props;
    }

    @OnDisabled
    public void clearAll() {
        properties.clear();
    }
}
