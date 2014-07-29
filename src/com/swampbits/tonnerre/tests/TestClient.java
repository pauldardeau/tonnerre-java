/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.swampbits.tonnerre.tests;

import java.util.ArrayList;

import com.swampbits.chaudiere.KeyValuePairs;
import com.swampbits.tonnerre.Message;
import com.swampbits.tonnerre.Messaging;


/**
 *
 * @author paul
 */
public class TestClient {
   
   public static void PrintKeyValues(KeyValuePairs kvp) {
      ArrayList<String> keys = new ArrayList<>();
      kvp.getKeys(keys);
      for (String key : keys) {
         String value = kvp.getValue(key);
         System.out.println("key='" + key + "', value='" + value + "'");
      }
   }


   public static void main(String[] args) {
      //StdLogger logger = new StdLogger(Logger::LogLevel::Info));
      //logger->setLogInstanceLifecycles(true);
      //Logger::setLogger(logger);
   
      String SERVICE_SERVER_INFO = "server_info";
      String SERVICE_ECHO        = "echo_service";
      String SERVICE_STOOGE_INFO = "stooge_info_service";

      String serviceName;
      //serviceName = SERVICE_SERVER_INFO;
      serviceName = SERVICE_ECHO;
      //serviceName = SERVICE_STOOGE_INFO;

      try {
         Messaging.initialize("/Users/paul/github/tonnerre/test/tonnerre.ini");
         
         System.out.println("Messaging initialized");
   
         if (serviceName.equals(SERVICE_SERVER_INFO)) {
            Message message = new Message("serverInfo", Message.MessageType.Text);
            Message response = new Message();
            if (message.send(serviceName, response)) {
               String responseText = response.getTextPayload();
               System.out.println("response: '" + responseText + "'");
            } else {
               System.out.println("error: unable to send message to service " + serviceName);
            }
         } else if (serviceName.equals(SERVICE_ECHO)) {
            KeyValuePairs kvp = new KeyValuePairs();
            kvp.addPair("firstName", "Mickey");
            kvp.addPair("lastName", "Mouse");
            kvp.addPair("city", "Orlando");
            kvp.addPair("state", "FL");

            Message message = new Message("echo", Message.MessageType.KeyValues);
            message.setKeyValuesPayload(kvp);
            Message response = new Message();
            if (message.send(serviceName, response)) {
               KeyValuePairs responseKeyValues = response.getKeyValuesPayload();
               PrintKeyValues(responseKeyValues);
            } else {
               System.out.println("error: unable to send message to service " + serviceName);
            }
         } else if (serviceName.equals(SERVICE_STOOGE_INFO)) {
            Message message = new Message("listStooges", Message.MessageType.KeyValues);
            Message response = new Message();
            if (message.send(serviceName, response)) {
               KeyValuePairs responseKeyValues = response.getKeyValuesPayload();
               PrintKeyValues(responseKeyValues);
            } else {
               System.out.println("error: unable to send message to service " + serviceName);
            }
         } else {
            System.out.println("unrecognized serviceName: " + serviceName);
         }
      }
      catch (Exception e)
      {
         System.out.println("Exception caught: " + e.getMessage());
      }
   }
 
}
