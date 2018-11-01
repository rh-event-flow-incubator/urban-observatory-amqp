/*
 * Copyright 2018 Red Hat.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.eventflow.demos.sources.ws;

import java.net.URI;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

/**
 *  This bridge receives WS events and passes them onto an AMQ connection
 * @author hhiden
 */
public class WSAMQBridge {
    private final Logger logger = Logger.getLogger(WSAMQBridge.class.getName());
    
    WebSocketClient client;
    private MessageFormatter formatter;
    private URI wsUri;
    private String amqpUri;
    private volatile boolean shutdownSet = false;
    private Connection amqpConnection;
    private Session session;
    private MessageProducer producer;
    
    public WSAMQBridge(URI wsUri, String ampqUri) {
        this.wsUri = wsUri;
        this.amqpUri = ampqUri;
    }

    public void setFormatter(MessageFormatter formatter) {
        this.formatter = formatter;
    }
    
    private void connectJMS() throws Exception {
        Hashtable env = new Hashtable();
        env.put("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        env.put("connectionfactory.myFactoryLookup",amqpUri);
        env.put("topic.myTopicLookup","topic");
        InitialContext ctx = new InitialContext(env);
        ConnectionFactory factory = (ConnectionFactory) ctx.lookup("myFactoryLookup");
        if(factory!=null){
            logger.info("Got AMQP factory");
            Destination queue = (Destination) ctx.lookup("myTopicLookup");
            amqpConnection = factory.createConnection();
            logger.info("Created AMQP connection");
            amqpConnection.start();
            session = amqpConnection.createSession();
            producer = session.createProducer(queue);
            logger.info("AMQP Setup OK");
        }
    }
    
    private void connectWS() throws InterruptedException {
        if(client==null){
            logger.info("Tring to connect WS client");
            WebSocketClient c = new WebSocketClient(wsUri) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    logger.info("WS Connection opened");
                }
                
                @Override
                public void onMessage(String message) {
                    if(formatter!=null){
                        Message msg = null;
                        try {
                           msg = formatter.format(message, session);
                        } catch (Exception e){
                            logger.log(Level.SEVERE, "Error converting WS message: " + e.getMessage(), e);
                        } 
                        
                        if(msg!=null){
                            try {
                                producer.send(msg);
                            } catch (Exception e){
                                logger.log(Level.SEVERE, "Error sending converted message: " + e.getMessage(), e);
                            }
                        }
                    }
                }
                
                @Override
                public void onClose(int code, String reason, boolean remote) {
                    logger.info("Close");
                    clientClosed();
                }
               
                @Override
                public void onError(Exception ex) {
                    logger.log(Level.SEVERE, "WS Error", ex);
                }
            };
            c.connectBlocking();
            this.client = c;
        }
    }

    private void clientClosed(){
        this.client = null;
    }
    
    public void startBridge() throws Exception {
        connectJMS();
        connectWS();
    }
    
    public void stopBridge() throws Exception {
        if(client!=null){
            logger.info("Closing WS Connection");
            client.closeBlocking();
            client = null;
            logger.info("WS Connection closed");
        }
        
        if(producer!=null){
            logger.info("Closing AMQP Producer");
            producer.close();
            logger.info("AMQP Producer Closed");
        }
        
        if(session!=null){
            logger.info("Closing AMQP Session");
            session.close();
            logger.info("AMQP Session closed");
        }
        
        if(amqpConnection!=null){
            logger.info("Closing AMQP Connection");
            amqpConnection.close();
            logger.info("AMQP Connection closed");
        }
            
    }
    
    
}
