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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;


/**
 * This class provides a local ActiveMQ server that can be connected to from 
 * Localhost
 * @author hugo
 */
public class LocalActiveMQServer {
    Logger logger = Logger.getLogger(LocalActiveMQServer.class.getName());
    ManagementContext mc;
    BrokerService broker;
    private volatile boolean running;    
    
    public void startServer() {
        if (!running) {
            logger.info("Starting AMQP server");
            broker = new BrokerService();
            try {
                broker.addConnector("amqp://127.0.0.1:5672");
                broker.setUseJmx(false);
                broker.start();
                running = true;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error starting JMS Server: " + e.getMessage(), e);
                try {
                    broker.stop();
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, "Error stopping JMS Server: " + e.getMessage(), e);
                }
                running = false;
            }
        } else {
            logger.log(Level.SEVERE, "JMS Server is already running");
        }
    }

    public void stopServer() {
        if (running && broker != null) {
            logger.info("Stopping AMQP Server");
            try {
                broker.stop();
                broker = null;
                running = false;
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Error stopping JMS Server: " + ex.getMessage(), ex);
            }
        }
    }

    public boolean isRunning() {
        return running;
    }    
}