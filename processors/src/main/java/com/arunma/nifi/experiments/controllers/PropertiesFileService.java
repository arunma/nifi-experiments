package com.arunma.nifi.experiments.controllers;

import org.apache.nifi.controller.ControllerService;

import java.util.Map;

public interface PropertiesFileService {
    String getProperty(String key);
    Map<String,String> getAllProperties();
}
