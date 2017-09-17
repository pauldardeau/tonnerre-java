/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.swampbits.tonnerre;

import java.util.HashMap;
import java.util.List;

import com.swampbits.chaudiere.IniReader;
import com.swampbits.chaudiere.KeyValuePairs;
import com.swampbits.chaudiere.Logger;
import com.swampbits.chaudiere.ServiceInfo;


/**
 * Messaging class provides a global entry point for initializing the messaging system
 * @author paul
 */
public class Messaging {
   private static final String KEY_SERVICES = "services";
   private static final String KEY_HOST     = "host";
   private static final String KEY_PORT     = "port";
   
   
   private static Messaging messagingInstance = null;
   private final HashMap<String, ServiceInfo> mapServices;    

    
   /**
    * Establishes a Messaging instance as the singleton for messaging
    * @param messaging the Messaging object instance for messaging
    */
   public static void setMessaging(Messaging messaging) {
      messagingInstance = messaging;
   }
   
   /**
    * Retrieves the Messaging singleton instance
    * @return pointer to the Messaging instance, or null if not initialized
    */
   public static Messaging getMessaging() {
      return messagingInstance;
   }
   
   /**
    * Initializes the messaging system by reading the configuration file and creating a Messaging instance
    * @param configFilePath the file path to the INI configuration file
    * @throws Exception
    */
   public static void initialize(String configFilePath) throws Exception {
      Logger.debug("Messaging.initialize: reading configuration file");
      IniReader reader = new IniReader(configFilePath);
      
      if (reader.hasSection(KEY_SERVICES)) {
         KeyValuePairs kvpServices = new KeyValuePairs();
         if (reader.readSection(KEY_SERVICES, kvpServices)) {
            List<String> keys = kvpServices.getKeys();
            int servicesRegistered = 0;
         
            Messaging messaging = new Messaging();
         
            for (String key : keys) {
               String serviceName = key;
               String sectionName = kvpServices.getValue(serviceName);
            
               KeyValuePairs kvp = new KeyValuePairs();
               if (reader.readSection(sectionName, kvp)) {
                  if (kvp.hasKey(KEY_HOST) && kvp.hasKey(KEY_PORT)) {
                     String host = kvp.getValue(KEY_HOST);
                     String portAsString = kvp.getValue(KEY_PORT);
                     final short portValue = Short.parseShort(portAsString);
                  
                     ServiceInfo serviceInfo = new ServiceInfo(serviceName, host, portValue);
                     messaging.registerService(serviceName, serviceInfo);
                     ++servicesRegistered;
                  }
               }
            }
         
            if (servicesRegistered > 0) {
               Messaging.setMessaging(messaging);
               Logger.info("Messaging initialized");
            } else {
               Logger.debug("Messaging.initialize: no services registered");
               throw new Exception("no services registered");
            }
         }
      }       
   }
   
   /**
    * Default constructor
    */
   public Messaging() {
      mapServices = new HashMap<>();
   }
   
   /**
    * Determines if the messaging system has been initialized
    * @return boolean indicating if messaging system has been initialized
    */
   public static boolean isInitialized() {
      return null != getMessaging();
   }
   
   /**
    * Registers a service with its name and host/port values
    * @param serviceName the name of the service being registered
    * @param serviceInfo the host/port values for the service
    * @see ServiceInfo()
    */
   public void registerService(String serviceName, ServiceInfo serviceInfo) {
      mapServices.put(serviceName, serviceInfo);
   }
   
   /**
    * Determines if the specified service name has been registered
    * @param serviceName the service name whose existence is being evaluated
    * @return boolean indicating if the service has been registered
    */
   public boolean isServiceRegistered(String serviceName) {
      return mapServices.containsKey(serviceName);
   }
   
   /**
    * Retrieves the host and port values for the specified service name
    * @param serviceName the name of the service whose host/port values are being requested
    * @return object holding the host/port values for the service
    * @see ServiceInfo()
    */
   public ServiceInfo getInfoForService(String serviceName) {
      return mapServices.get(serviceName);
   }

}
