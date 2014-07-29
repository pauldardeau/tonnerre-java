/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.swampbits.tonnerre;

import java.util.ArrayList;
import java.util.StringTokenizer;

import com.swampbits.chaudiere.KeyValuePairs;
import com.swampbits.chaudiere.Logger;
import com.swampbits.chaudiere.ServiceInfo;
import com.swampbits.chaudiere.Socket;
import com.swampbits.chaudiere.StrUtils;


/**
 * The Message class is the primary object used for sending and receiving messages.
 * A new instance should be created for each message to send.  The messaging server
 * automatically creates the message instance (populated by the message handler
 * associated with the service) for the response.
 * @author paul
 */
public class Message {
    
   private static final String EMPTY_STRING           = "";

   private static final int MAX_SEGMENT_LENGTH        = 32767;

   private static final int NUM_CHARS_HEADER_LENGTH   = 10;

   private static final String DELIMITER_KEY_VALUE    = "=";
   private static final String DELIMITER_PAIR         = ";";

   private static final String KEY_ONE_WAY            = "1way";
   private static final String KEY_PAYLOAD_LENGTH     = "payload_length";
   private static final String KEY_PAYLOAD_TYPE       = "payload_type";
   private static final String KEY_REQUEST_NAME       = "request";

   private static final String VALUE_PAYLOAD_KVP      = "kvp";
   private static final String VALUE_PAYLOAD_TEXT     = "text";
   private static final String VALUE_PAYLOAD_UNKNOWN  = "unknown";
   private static final String VALUE_TRUE             = "true";


   private String m_serviceName;
   private String m_textPayload;
   private KeyValuePairs m_kvpPayload;
   private KeyValuePairs m_kvpHeaders;
   private MessageType m_messageType;
   private boolean m_isOneWay;    

    
   public enum MessageType {
      Unknown,
      KeyValues,
      Text
   };
   
   /**
    * Reconstructs a message by reading from a socket
    * @param socket the socket to read from
    * @return a new Message object instance constructed by reading data from socket
    * @see Socket()
    */
   public static Message reconstruct(Socket socket) {
      if ((socket != null) && socket.isOpen()) {
         Message message = new Message();
         if (message.reconstitute(socket)) {
            return message;
         } 
      }
      
      return null;
   }
   
   /**
    * Default constructor (used internally)
    */
   public Message() {
      m_messageType = MessageType.Unknown;
      m_isOneWay = false;
      m_kvpHeaders = new KeyValuePairs();
   }
   
   /**
    * Constructs a message in anticipation of sending it
    * @param requestName the name of the message request
    * @param messageType the type of the message
    */
   public Message(String requestName, MessageType messageType) {
      m_messageType = messageType;
      m_isOneWay = false;
      m_kvpHeaders = new KeyValuePairs();
      m_kvpHeaders.addPair(KEY_REQUEST_NAME, requestName);
   }
   
   /**
    * Sends a message to the specified service and disregards any response that the
    * server handler might generate.
    * @param serviceName the name of the service destination
    * @return boolean indicating if message was successfully delivered
    */
   public boolean send(String serviceName) {
      if (m_messageType == MessageType.Unknown) {
         Logger.error("unable to send message, no message type set");
         return false;
      }

      Socket socket = socketForService(serviceName);
   
      if (socket != null) {
         m_isOneWay = true;
      
         if (socket.write(toString())) {
            return true;
         } else {
            // unable to write to socket
            Logger.error("unable to write to socket");
         }
      } else {
         // unable to connect to service
         Logger.error("unable to connect to service");
      }
   
      return false;       
   }
   
   /**
    * Sends a message and retrieves the message response (synchronous call)
    * @param serviceName the name of the service destination
    * @param responseMessage the message object instance to populate with the response
    * @return boolean indicating if the message was successfully delivered and a response received
    */
   public boolean send(String serviceName, Message responseMessage) {
      if (m_messageType == MessageType.Unknown) {
         Logger.error("unable to send message, no message type set");
         return false;
      }
   
      Socket socket = socketForService(serviceName);
   
      if (socket != null) {
         if (Logger.isLogging(Logger.LogLevel.Verbose)) {
            String payload = toString();
            Logger.verbose("payload: '" + payload + "'");
         }
      
         if (socket.write(toString())) {
            return responseMessage.reconstitute(socket);
         } else {
            // unable to write to socket
            Logger.error("unable to write to socket");
         }
      } else {
         // unable to connect to service
         Logger.error("unable to connect to service");
      }

      return false;       
   }
   
   /**
    * Reconstitute a message by reading message state data from a socket (used internally)
    * @param socket the socket from which to read message state data
    * @return boolean indicating whether the message was successfully reconstituted
    * @see Socket()
    */
   public boolean reconstitute(Socket socket) {
      if (socket != null) {
         if (m_kvpHeaders == null) {
            m_kvpHeaders = new KeyValuePairs();
         }
         
         char[] headerLengthPrefixBuffer = new char[NUM_CHARS_HEADER_LENGTH];
      
         if (socket.readSocket(headerLengthPrefixBuffer, NUM_CHARS_HEADER_LENGTH)) {
         
            String headerLengthPrefix = new String(headerLengthPrefixBuffer);
            headerLengthPrefix = StrUtils.stripTrailing(headerLengthPrefix, ' ');
            
            Logger.verbose("headerLengthPrefix read: '" + headerLengthPrefix + "'");
            
            final int headerLength = Integer.parseInt(headerLengthPrefix);
         
            if (headerLength > 0) {
               char[] headerBuffer = new char[headerLength];
               String headerAsString;
               
               if (socket.readSocket(headerBuffer, headerLength)) {
                  headerAsString = new String(headerBuffer);
               } else {
                  Logger.error("reading socket for header failed");
                  return false;
               }
            
               if (headerAsString.length() > 0) {
                  if (fromString(headerAsString, m_kvpHeaders)) {
                     if (m_kvpHeaders.hasKey(KEY_PAYLOAD_TYPE)) {
                        final String valuePayloadType = m_kvpHeaders.getValue(KEY_PAYLOAD_TYPE);
                     
                        if (valuePayloadType.equals(VALUE_PAYLOAD_TEXT)) {
                           m_messageType = MessageType.Text;
                        } else if (valuePayloadType.equals(VALUE_PAYLOAD_KVP)) {
                           m_messageType = MessageType.KeyValues;
                        }
                     }
                  
                     if (m_messageType == MessageType.Unknown) {
                        Logger.error("unable to identify message type from header");
                        return false;
                     }
                  
                     if (m_kvpHeaders.hasKey(KEY_PAYLOAD_LENGTH)) {
                        final String valuePayloadLength = m_kvpHeaders.getValue(KEY_PAYLOAD_LENGTH);
                     
                        if (valuePayloadLength.length() > 0) {
                           final int payloadLength = Integer.parseInt(valuePayloadLength);
                        
                           if ((payloadLength > 0) && (payloadLength <= MAX_SEGMENT_LENGTH)) {
                              String payloadAsString;
                              char[] payloadBuffer = new char[payloadLength];
                              if (socket.readSocket(payloadBuffer, payloadLength)) {
                                 payloadAsString = new String(payloadBuffer);
                              } else {
                                 Logger.error("reading socket for payload failed");
                                 return false;
                              }
                           
                              if (payloadAsString.length() > 0) {
                                 if (m_messageType == MessageType.Text) {
                                    m_textPayload = payloadAsString;
                                 } else if (m_messageType == MessageType.KeyValues) {
                                    m_kvpPayload = new KeyValuePairs();
                                    fromString(payloadAsString, m_kvpPayload);
                                 }
                              }
                           }
                        }
                     }
                  
                     if (m_kvpHeaders.hasKey(KEY_ONE_WAY)) {
                        final String valueOneWay = m_kvpHeaders.getValue(KEY_ONE_WAY);
                        if (valueOneWay.equals(VALUE_TRUE)) {
                           // mark it as being a 1-way message
                           m_isOneWay = true;
                        }
                     }
                  
                     return true;
                  } else {
                     // unable to parse header
                     Logger.error("unable to parse header");
                  }
               } else {
                  // unable to read header
                  Logger.error("unable to read header");
               }
            } else {
               // header length is empty
               Logger.error("header length is empty");
            }
         } else {
            // socket read failed
            Logger.error("socket read failed");
         }
      } else {
         // no socket given
         Logger.error("no socket given to reconstitute");
      }

      return false;       
   }
   
   /**
    * Sets the type of the message
    * @param messageType the type of the message
    */
   public void setType(MessageType messageType) {
      m_messageType = messageType;
   }
   
   /**
    * Retrieves the type of the message
    * @return the message type
    */
   public MessageType getType() {
      return m_messageType;
   }
   
   /**
    * Retrieves the name of the message request
    * @return the name of the message request
    */
   public String getRequestName() {
      if ((m_kvpHeaders != null) && m_kvpHeaders.hasKey(KEY_REQUEST_NAME)) {
         return m_kvpHeaders.getValue(KEY_REQUEST_NAME);
      } else {
         return EMPTY_STRING;
      }
   }
   
   /**
    * Retrieves the key/values payload associated with the message
    * @return reference to the key/values message payload
    * @see KeyValuePairs()
    */
   public KeyValuePairs getKeyValuesPayload() {
      return m_kvpPayload;
   }
   
   /**
    * Retrieves the textual payload associated with the message
    * @return reference to the textual message payload
    */
   public String getTextPayload() {
      return m_textPayload;
   }
   
   /**
    * Sets the key/values payload associated with the message
    * @param kvp the new key/values payload
    * @see KeyValuePairs()
    */
   public void setKeyValuesPayload(KeyValuePairs kvp) {
      m_kvpPayload = kvp;
   }
   
   /**
    * Sets the textual payload associated with the message
    * @param text the new textual payload
    */
   public void setTextPayload(String text) {
      m_textPayload = text;
   }

   /**
    * Retrieves the service name from a reconstituted message (used internally)
    * @return the name of the service
    */
   public String getServiceName() {
      return m_serviceName;
   }
   
   /**
    * Flatten the message state to a string so that it can be sent over network connection (used internally)
    * @return string representation of message state ready to be sent over network
    */
   @Override
   public String toString() {
      KeyValuePairs kvpHeaders = new KeyValuePairs(m_kvpHeaders);
      String payload = "";
   
      if (m_messageType == MessageType.Text) {
         kvpHeaders.addPair(KEY_PAYLOAD_TYPE, VALUE_PAYLOAD_TEXT);
         payload = m_textPayload;
      } else if (m_messageType == MessageType.KeyValues) {
         kvpHeaders.addPair(KEY_PAYLOAD_TYPE, VALUE_PAYLOAD_KVP);
         payload = toString(m_kvpPayload);
      } else {
         kvpHeaders.addPair(KEY_PAYLOAD_TYPE, VALUE_PAYLOAD_UNKNOWN);
      }
   
      if (m_isOneWay) {
         kvpHeaders.addPair(KEY_ONE_WAY, VALUE_TRUE);
      }
   
      if (m_kvpHeaders.hasKey(KEY_REQUEST_NAME)) {
         kvpHeaders.addPair(KEY_REQUEST_NAME, m_kvpHeaders.getValue(KEY_REQUEST_NAME));
      } else {
         kvpHeaders.addPair(KEY_REQUEST_NAME, "");
      }

      kvpHeaders.addPair(KEY_PAYLOAD_LENGTH, Integer.toString(payload.length()));
   
      final String headersAsString = toString(kvpHeaders);
   
      String headerLengthPrefix = encodeLength(headersAsString.length());
      headerLengthPrefix = StrUtils.padRight(headerLengthPrefix, ' ', NUM_CHARS_HEADER_LENGTH);

      StringBuilder messageAsString = new StringBuilder();
      messageAsString.append(headerLengthPrefix);
      messageAsString.append(headersAsString);
      messageAsString.append(payload);
   
      return messageAsString.toString();
   }
   
   /**
    * Flatten a KeyValuePairs object as part of flattening the Message
    * @param kvp the KeyValuePairs object whose string representation is needed
    * @return the string representation of the KeyValuePairs object
    */
   public static String toString(KeyValuePairs kvp) {
      StringBuilder kvpAsString = new StringBuilder();
   
      if ((kvp != null) && !kvp.empty()) {
         ArrayList<String> keys = new ArrayList<>();
         kvp.getKeys(keys);
         int i = 0;
      
         for (String key : keys) {
            if (i > 0) {
               // append pair delimiter
               kvpAsString.append(DELIMITER_PAIR);
            }
         
            kvpAsString.append(key);
            kvpAsString.append(DELIMITER_KEY_VALUE);
            kvpAsString.append(kvp.getValue(key));
         
            ++i;
         }
      }
      
      return kvpAsString.toString();
   }
   
   /**
    * Reconstitutes the state of a KeyValuePairs from the specified string
    * @param s the textual data that holds the KeyValuePairs state data
    * @param kvp the KeyValuePairs object instance to populate
    * @return boolean indicating whether any state data was populated
    * @see KeyValuePairs()
    */
   public static boolean fromString(String s, KeyValuePairs kvp) {
      int numPairsAdded = 0;
   
      if ((s != null) && (kvp != null) && (s.length() > 0)) {
         StringTokenizer stPairs = new StringTokenizer(s, DELIMITER_PAIR);
         final int numPairs = stPairs.countTokens();
      
         if (numPairs > 0) {
            while (stPairs.hasMoreTokens()) {
               String keyValuePair = stPairs.nextToken();
            
               StringTokenizer stKeyValue = new StringTokenizer(keyValuePair, DELIMITER_KEY_VALUE);
               final int numTokens = stKeyValue.countTokens();
               if (numTokens == 2) {
                  String key = stKeyValue.nextToken();
                  String value = stKeyValue.nextToken();
                  kvp.addPair(key, value);
                  ++numPairsAdded;
               }
            }
         }
      }
   
      return numPairsAdded > 0;
   }
   
   /**
    * Encodes a length to a string so that it can be encoded in flattened message (used internally)
    * @param lengthBytes the length in bytes to encode
    * @return the string representation of the length
    */
   public static String encodeLength(int lengthBytes) {
      return StrUtils.padRight(Integer.toString(lengthBytes),
                               ' ',
                               NUM_CHARS_HEADER_LENGTH);
   }
   
   /**
    * Decodes the length of the message header by reading from a socket (used internally)
    * @param socket the socket to read from
    * @return the decoded length of the message header
    * @see Socket()
    */
   public static int decodeLength(Socket socket) {
      int lengthBytes = 0;
   
      if (socket != null) {
         char[] lengthAsChars = new char[NUM_CHARS_HEADER_LENGTH+1];
         if (socket.readSocket(lengthAsChars, NUM_CHARS_HEADER_LENGTH)) {
            String encodedLength = new String(lengthAsChars);
            return Integer.parseInt(encodedLength);
         }
      }
   
      return lengthBytes;       
   }
   
   /**
    * Retrieves a socket connection for the specified service (used internally)
    * @param serviceName the name of the service whose connection is needed
    * @return a Socket instance on success, null on failure
    */
   public Socket socketForService(String serviceName) {
      if (Messaging.isInitialized()) {
         Messaging messaging = Messaging.getMessaging();
      
         if (messaging != null) {
            if (messaging.isServiceRegistered(serviceName)) {
               ServiceInfo serviceInfo = messaging.getInfoForService(serviceName);
               String host = serviceInfo.host();
               final short port = serviceInfo.port();
               Socket socket;
               try {
                  socket = new Socket(host, port);
               } catch (Exception e) {
                  socket = null;
               }
               
               return socket;
            } else {
               Logger.error("service is not registered");
            }
         } else {
            Logger.error("Messaging.getMessaging returned null, but isInitialized returns true");
         }
      } else {
         Logger.error("messaging not initialized");
      }
   
      return null;       
   }
   
}
