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
package io.eventflow.demos.sources.ws.test;

import io.eventflow.demos.sources.ws.MessageFormatter;
import io.eventflow.demos.sources.ws.UrbanObservatoryFormatter;
import io.eventflow.demos.sources.ws.WSAMQBridge;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.CloudEventBuilder;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.logging.Logger;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author hhiden
 */
public class UrbanObservatoryTest {
    private static final Logger logger = Logger.getLogger(UrbanObservatoryTest.class.getName());
    private static LocalActiveMQServer server = new LocalActiveMQServer();
    private static AMQPListener listener;
    public UrbanObservatoryTest() {
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        logger.info("Starting JMS Server");
        server.startServer();
        listener = new AMQPListener(new URI("amqp://127.0.0.1:5672"));
        listener.connectJMS();
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        listener.disconnect();
        server.stopServer();
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void test() throws Exception {
        WSAMQBridge bridge = new WSAMQBridge(
                new URI("wss://api.usb.urbanobservatory.ac.uk/stream"),
                "amqp://127.0.0.1:5672"
        );
        
        bridge.setFormatter(new UrbanObservatoryFormatter());
        
        bridge.startBridge();
        
        Thread.sleep(10000);
        bridge.stopBridge();
    }
}
