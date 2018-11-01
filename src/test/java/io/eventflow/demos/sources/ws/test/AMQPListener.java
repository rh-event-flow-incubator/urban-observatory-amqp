/*
 * Copyright 2018 hhiden.
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
package io.eventflow.demos.sources.ws.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import java.net.URI;
import java.util.Hashtable;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Listens for AMQP Messages and logs them
 * @author hhiden
 */
public class AMQPListener implements MessageListener {
    private static final Logger logger = Logger.getLogger(AMQPListener.class.getName());
    private ObjectMapper mapper = new ObjectMapper();
    private URI amqpUri;
    private Connection amqpConnection;
    private Session session;
    private MessageConsumer consumer;

    public AMQPListener(URI amqpUri) {
        this.amqpUri = amqpUri;
    }
    
    public void connectJMS() throws Exception {
        Hashtable env = new Hashtable();
        env.put("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        env.put("connectionfactory.myFactoryLookup","amqp://localhost:5672");
        env.put("queue.myQueueLookup", "queue");
        env.put("topic.myTopicLookup","topic");
        InitialContext ctx = new InitialContext(env);
        ConnectionFactory factory = (ConnectionFactory) ctx.lookup("myFactoryLookup");
        if(factory!=null){
            logger.info("Got AMQP factory");
            Destination topic = (Destination) ctx.lookup("myTopicLookup");
            amqpConnection = factory.createConnection();
            logger.info("Created AMQP connection");
            amqpConnection.start();
            session = amqpConnection.createSession();
            consumer = session.createConsumer(topic);
            consumer.setMessageListener(this);
            logger.info("AMQP Setup OK");
        }
    }    

    public void disconnect() throws Exception {
        if(consumer!=null){
            logger.info("Closing AMQP Consumer");
            consumer.close();
            logger.info("AMQP Consumer Closed");
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
    @Override
    public void onMessage(Message msg) {
        try {
            CloudEvent evt = mapper.readValue(((TextMessage)msg).getText(), CloudEventImpl.class);
            String data = (String)evt.getData().get();
            JSONObject dataJson = new JSONObject(data);
            if(dataJson.has("unit")){
                logger.info(dataJson.getString("sensor") + 
                        " in " + dataJson.getString("location") + "=" + 
                        dataJson.getNumber("value") + 
                        " " + dataJson.getString("unit"));
                
            } else {
                logger.info(dataJson.getString("sensor") + 
                        " in " + dataJson.getString("location") + "=" + 
                        dataJson.getNumber("value"));
                
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    
}
