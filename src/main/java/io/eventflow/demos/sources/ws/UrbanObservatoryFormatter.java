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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.CloudEventBuilder;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.logging.Logger;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.json.JSONObject;

/**
 *
 * @author hhiden
 */
public class UrbanObservatoryFormatter implements MessageFormatter {
    private static final Logger logger = Logger.getLogger(UrbanObservatoryFormatter.class.getName());
    
    private ObjectMapper mapper = new ObjectMapper();

    public UrbanObservatoryFormatter() {
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);        
    }
    
    
    /**
     * Convert the WSJSON message to a TextMessage
     */
    @Override
    public Message format(String messageJson, Session session) throws Exception {
        JSONObject json = new JSONObject(messageJson);
        boolean readingOk = true;
        
        if (json.getInt("signal") > 0) {
            if (json.has("data")) {
                JSONObject dataObject = json.getJSONObject("data");
                ZonedDateTime eventTime = ZonedDateTime.now();

                JSONObject cloudEventData = new JSONObject();

                // Get the location
                if (dataObject.has("entity")) {
                    JSONObject entity = dataObject.getJSONObject("entity");
                    if(entity.has("name")){
                        cloudEventData.put("location", entity.getString("name"));
                    } else {
                        readingOk = false;
                    }
                    
                    if(entity.has("meta")){
                        cloudEventData.put("meta", entity.getJSONObject("meta"));
                    } 
                } else {
                    readingOk = false;
                }

                if(dataObject.has("feed")){
                    JSONObject feedJson = dataObject.getJSONObject("feed");
                    if(feedJson.has("metric")){
                        cloudEventData.put("sensor", feedJson.getString("metric"));
                    } else {
                        readingOk = false;
                    }
                } else {
                    readingOk = false;
                }
                
                if(dataObject.has("timeseries")){
                    JSONObject timeseriesJson = dataObject.getJSONObject("timeseries");
                    if(timeseriesJson.has("value")){
                        JSONObject valueJson = timeseriesJson.getJSONObject("value");
                        if(valueJson.has("data")){
                            // Value hser
                            cloudEventData.put("value", valueJson.getNumber("data"));
                            readingOk = true;
                        } else {
                            readingOk = false;
                        }
                        
                        if(valueJson.has("time")){
                            // Timestamp present
                            
                        }
                        
                        if(timeseriesJson.has("unit")){
                            String unit = timeseriesJson.getString("unit");
                            if(!"no units".equals(unit)){
                                cloudEventData.put("unit", timeseriesJson.getString("unit"));
                            }
                        }
                    } else {
                        readingOk = false;
                    }
                    
                } else {
                    readingOk = false;
                }
                
                if(readingOk){
                    final CloudEvent<String> buildingReading = new CloudEventBuilder<String>()
                            .eventTime(eventTime)
                            .eventType("reading")
                            .eventID(UUID.randomUUID().toString())
                            .source(new URI("wss://api.usb.urbanobservatory.ac.uk/stream"))
                            .data(cloudEventData.toString())
                            .build();

                    TextMessage msg = session.createTextMessage(mapper.writeValueAsString(buildingReading));
                    return msg;
                } else {
                    return null;
                }
            } else {
                return null;
            }

        } else {
            System.err.println("No signal");
            return null;
        }
    }    
}
