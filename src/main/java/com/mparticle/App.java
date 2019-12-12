package com.mparticle;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import retrofit2.*;
import com.mparticle.client.EventsApi;
import com.mparticle.model.*;



public class App {

    public static class Program {
        private EventsApi api;

        public static final boolean USE_KAFKA = false;
        public static final String BOOTSTRAP_SERVERS = "localhost:9092";
        public static final String TOPIC = "topic";
        public static final String API_KEY = "REPLACEME";
        public static final String API_SECRET = "REPLACEME";

        Program() {
            /*
            Create an EventsApi Instance
            ===============================
            EventsApi is a Retrofit-compatible interface,
             allowing you to use the rich feature-set of the Retrofit
             and OkHttp libraries, such as queueing and asynchronous requests.

            Create an API instance with your mParticle workspace credentials.
            These credentials may be either "platform" (iOS, Android, etc)
            or "custom feed" keys:
             */

            api = new ApiClient(API_KEY, API_SECRET).createService(EventsApi.class);
        }

        void run() throws Exception {
            //Kafka consumer configuration settings
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "hostname");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

            //Kafka Consumer subscribes list of topics here.
            consumer.subscribe(Arrays.asList(TOPIC));

            //print the topic name
            System.out.println("Subscribed to topic " + TOPIC);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String json = record.value();
                    sendEventToMParticle(json);

                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                }
            }
        }

        void testRun() throws Exception {
            String[] stream = {
                "{ \"event_type\" : \"identity\", \"customer_id\" : \"123\", \"properties\": { \"email\" : \"john.doe@gmail.com\", \"first_name\" : \"John\", \"last_name\" : \"Doe\", \"idfa\" : \"5864e6b0-0d46-4667-a463-21d9493b6c10\" } }",
                "{\"event_type\":\"page\",\"user_id\":\"123\",\"properties\":{\"category\":\"electronics\",\"subcategory\":\"televisions\"}}",
                "{ \"event_type\" : \"event\", \"event_name\" : \"add_to_cart\", \"user_id\" : \"123\", \"properties\": { \"currency_code\": \"USD\", \"price\": \"1000\", \"name\": \"Acme 42\\\" LED TV\", \"transaction_id\": \"abcdef12345\", \"sku\": \"12345\" } }"
            };

            for (String event : stream) {
                String json = event;
                Response result = sendEventToMParticle(json);
                if (result.isSuccessful()) {
                    System.out.println("Test run successful!");
                } else {
                    System.out.println("Test run failed");
                    System.out.println(result.toString());
                }
            }
        }

        private Response sendEventToMParticle(String json) throws Exception {
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
            Batch batch = createEvent(jsonObject);
            return uploadEventToMParticle(batch);
        }

        private Batch createEvent(JsonObject jsonObject)
        {
            String eventType = jsonObject.get("event_type").getAsString();
            JsonObject properties = jsonObject.get("properties").getAsJsonObject();


            Batch batch = new Batch();
            batch.environment(Batch.Environment.DEVELOPMENT); // or Batch.Environment.PRODUCTION

            /*
                Supply Identities
                =============
                It's critical to include either user or device identities with your server-side data
            */
            UserIdentities userIdentities = new UserIdentities();
            if (jsonObject.has("user_id")) {
                userIdentities.customerId(
                        jsonObject.get("user_id").getAsString()
                );
            }

            if (properties.has("email")) {
                userIdentities.customerId(
                        properties.get("email").getAsString()
                );
            }
            batch.userIdentities(userIdentities);

            if (properties.has("idfa")) {
                batch.deviceInfo(
                        new DeviceInformation()
                                .iosAdvertisingId(properties.get("idfa").getAsString())
                                // (or supply other IDs such as GAID)
                                // .androidAdvertisingId()
                );

            }

            /*
                Create Events
                =============
                All mParticle events have a similar structure:

                    event_type: this is the type of event, such as custom_event and commerce_event

                    data: this contains common properties of all events, as well as properties specific to each event_type

                The following are common properties that all events share, as represented by the
                 CommonEventData class:

                {
                    "data" :
                    {
                        "event_id" : 6004677780660780000,
                        "source_message_id" : "e8335d31-2031-4bff-afec-17ffc1784697",
                        "session_id" : 4957918240501247982,
                        "session_uuid" : "91b86d0c-86cb-4124-a8b2-edee107de454",
                        "timestamp_unixtime_ms" : 1402521613976,
                        "location" : {},
                        "device_current_state" : {},
                        "custom_attributes": {},
                        "custom_flags": {}
                    },
                    "event_type" : "custom_event"
                }

                The Java Server Events SDK represents this structure via an event
                and an event-data class for each unique event type. For example,
                CustomEvent which can be populated by a CustomEventData instance.
            */

            Object event = null;
            CustomEvent customEvent;
            Map customAttributes;
            switch(eventType) {
                case "page":
                    // The mParticle UI populates with page names (and event names).
                    // This includes data filtering, for example. If there are 1000 or
                    // more unique page names (as is often the case with unique product
                    // urls, for example), you should include the page name as a custom
                    // attribute, and use the screen name property of the event to denote
                    // a more general section of the site/app.

                    String category = properties.get("category").getAsString();
                    String subcategory = properties.get("subcategory").getAsString();
                    ScreenViewEvent screenEvent = new ScreenViewEvent().data(
                            new ScreenViewEventData()
                                .screenName(category)
                    );
                    customAttributes = new HashMap<>();
                    customAttributes.put("page_subcategory", subcategory);
                    event = screenEvent;
                    break;

                case "event":
                    String eventName = jsonObject.get("event_name").getAsString();

                    if (eventName.equals("add_to_cart")) {
                        String productId = properties.get("sku").getAsString();
                        String productName = properties.get("name").getAsString();
                        BigDecimal productCost = properties.get("price").getAsBigDecimal();
                        String transactionId = properties.get("transaction_id").getAsString();
                        String currencyCode = properties.get("currency_code").getAsString();

                        Product product = new Product()
                                .totalProductAmount(productCost)
                                .id(productId)
                                .name(productName);
                        ProductAction action = new ProductAction()
                                .action(ProductAction.Action.ADD_TO_CART)
                                .totalAmount(productCost)
                                .transactionId(transactionId)
                                .products(Arrays.asList(product));
                        event = new CommerceEvent().data(
                                new CommerceEventData()
                                        .productAction(action)
                                        .currencyCode(currencyCode)
                        );
                    } else {
                        // map other events
                    }
                    break;
                case "identity":
                    String firstName = properties.get("first_name").getAsString();
                    String lastName = properties.get("last_name").getAsString();
                    customAttributes = new HashMap<>();
                    customAttributes.put("first_name", firstName);
                    customAttributes.put("last_name", lastName);
                    customEvent = new CustomEvent().data(
                            new CustomEventData()
                                    .eventName("login")
                                    .customEventType(CustomEventData.CustomEventType.USER_CONTENT)
                    );
                    customEvent.getData().customAttributes(customAttributes);
                    event = customEvent;
                    break;
            }

            batch.addEventsItem(event);
            return batch;
        }

        private retrofit2.Response uploadEventToMParticle(Batch batch) throws Exception {
            retrofit2.Call<Void> singleResult = api.uploadEvents(batch);
            retrofit2.Response<Void> response = singleResult.execute();
            return response;
            // Should return result code - 202
        }

        /**
         * To maximize throughput, it's ideal to include multiple user events per batch, and multiple batches per upload.
         */
        private retrofit2.Response uploadBulkEventsToMParticle(List<Batch> batch) throws Exception {
            retrofit2.Call<Void> result = api.bulkUploadEvents(batch);
            retrofit2.Response<Void> response = result.execute();
            return response;
            // Should return result code - 202
        }
    }

    public static void main(String[] args) throws Exception {
        Program program = new Program();

        if (Program.USE_KAFKA) {
            program.run();
        } else {
            program.testRun();
        }
    }
}