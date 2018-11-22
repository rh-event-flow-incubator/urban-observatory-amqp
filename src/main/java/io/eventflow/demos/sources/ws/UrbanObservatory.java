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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.CloudEventBuilder;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.json.JSONObject;

/**
 * Connects the Urban observatory to a AMQP server
 *
 * @author hhiden
 */
public class UrbanObservatory {
    private static final Logger logger = Logger.getLogger(UrbanObservatory.class.getName());
    
    
    public static void main(String[] args) {
        // Set default System.properties
        System.setProperty("WS_URI", "wss://api.usb.urbanobservatory.ac.uk/stream");
        System.setProperty("AMQP_URI", "amqp://127.0.0.1:5672");

        String wsUri = EnvironmentResolver.get("WS_URI");
        String amqpUri = EnvironmentResolver.get("AMQP_URI");

        logger.info("Source: " + wsUri);
        logger.info("Target: " + amqpUri);
        try {

            final WSAMQBridge bridge = new WSAMQBridge(new URI(wsUri), amqpUri);
            bridge.setFormatter(new UrbanObservatoryFormatter());    // Formats the data
            
            // Tidy up ob shutdown if the bridge starts
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("SHUTDOWN");
                try {
                    bridge.stopBridge();
                    System.exit(0);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error shutting down bridge: " + e.getMessage(), e);
                    System.exit(1);
                }
            }));
            bridge.startBridge();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error setting up bridge: " + e.getMessage(), e);
            System.exit(1);
        }

    }



}
