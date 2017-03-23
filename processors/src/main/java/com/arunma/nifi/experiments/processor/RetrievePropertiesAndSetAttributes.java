package com.arunma.nifi.experiments.processor;

import com.arunma.nifi.experiments.controllers.PropertiesFileControllerService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Arun
 */
public class RetrievePropertiesAndSetAttributes extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RetrievePropertiesAndSetAttributes.class);

    static final PropertyDescriptor PROPERTY_FILE_SERVICE =new PropertyDescriptor.Builder()
            .name("Property File Service")
            .description("Property file service")
            .required(true)
            .build();


    static final Relationship REL_SUCCESS=new Relationship.Builder()
            .name("success")
            .description("If the properties are all set as attributes")
            .build();


    static final Relationship REL_FAILURE=new Relationship.Builder()
            .name("failure")
            .description("Exception thrown when setting the attributes to the flow")
            .build();


    private static List<PropertyDescriptor> descriptors;
    private static Set<Relationship> relationships;


    static{
        List<PropertyDescriptor> props=new ArrayList<>();
        props.add(PROPERTY_FILE_SERVICE);
        descriptors= Collections.unmodifiableList(props);

        Set<Relationship> rels=new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships=Collections.unmodifiableSet(rels);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        PropertiesFileControllerService propsFileControllerService = context.getProperty(PROPERTY_FILE_SERVICE).asControllerService(PropertiesFileControllerService.class);
        Map<String, String> allProperties = propsFileControllerService.getAllProperties();

        FlowFile flowFile=session.get();
        if (flowFile==null) return;

        try {
            if (allProperties==null || allProperties.isEmpty()){
                logger.info("Propery file does not have any properties. Transferring flow to SUCCESS WITHOUT setting any properties");
            }
            else{
                session.putAllAttributes(flowFile, allProperties);
                logger.info ("All attributes set to the session");
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Unexpected exception has happened", e);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
