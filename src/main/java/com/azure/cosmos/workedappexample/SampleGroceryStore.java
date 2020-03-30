// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.workedappexample;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosPagedFlux;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.workedappexample.common.AccountSettings;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosAsyncContainerResponse;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.FeedOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.Console;

/**
 * End-to-end application example code using Change Feed.
 *
 * This sample application inserts grocery store inventory data into an Azure Cosmos DB container;
 * meanwhile, Change Feed runs in the background building a materialized view
 * based on each document update.
 *
 * The materialized view facilitates efficient queries over item type.
 *
 */
public class SampleGroceryStore {

    public static int WAIT_FOR_WORK = 60000;
    public static final String DATABASE_NAME = "GroceryStoreDatabase";
    public static final String COLLECTION_NAME = "InventoryContainer";
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(SampleGroceryStore.class.getSimpleName());


    private static ChangeFeedProcessor changeFeedProcessorInstance;
    private static AtomicBoolean isProcessorRunning = new AtomicBoolean(false);

    private static CosmosAsyncContainer feedContainer;
    private static CosmosAsyncContainer typeContainer;

    private static Console c = System.console();

    private static String idToDelete;

    public static void main (String[]args) {
        logger.info("BEGIN Sample");

        try {

            System.out.println("\n\n\n\nPress enter to create the grocery store inventory system...");
            c.readLine();

            logger.info("-->CREATE DocumentClient");
            CosmosAsyncClient client = getCosmosClient();

            logger.info("-->CREATE Contoso Grocery Store database: " + DATABASE_NAME);
            CosmosAsyncDatabase cosmosDatabase = createNewDatabase(client, DATABASE_NAME);

            logger.info("-->CREATE container for store inventory: " + COLLECTION_NAME);
            feedContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME, "/id");

            logger.info("-->CREATE container for lease: " + COLLECTION_NAME + "-leases");
            CosmosAsyncContainer leaseContainer = createNewLeaseCollection(client, DATABASE_NAME, COLLECTION_NAME + "-leases");

            logger.info("-->CREATE container for materialized view partitioned by 'type': " + COLLECTION_NAME + "-leases");
            typeContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME + "-pktype", "/type");

            System.out.println("\n\n\n\nPress enter to start creating the materialized view...");
            c.readLine();

            changeFeedProcessorInstance = getChangeFeedProcessor("SampleHost_1", feedContainer, leaseContainer);
            changeFeedProcessorInstance.start()
                .subscribeOn(Schedulers.elastic())
                .doOnSuccess(aVoid -> {
                    isProcessorRunning.set(true);
                })
                .subscribe();

            while (!isProcessorRunning.get()); //Wait for Change Feed processor start

            System.out.println("\n\n\n\nPress enter to insert 10 items into the container." + COLLECTION_NAME + "...");
            c.readLine();

            // Insert 10 documents into the feed container
            // createNewDocumentsJSON demonstrates how to insert a JSON object into a Cosmos DB container as an item
            createNewDocumentsJSON(feedContainer, Duration.ofSeconds(3));

            System.out.println("\n\n\n\nPress enter to delete item with id " + idToDelete + " (Jerry's plums)...");
            c.readLine();

            // Demonstrate deleting a document from the feed container and the materialized view, using TTL=0
            deleteDocument();

            System.out.println("\n\n\n\nPress ENTER to clean up & exit the sample code...");
            c.readLine();

            if (changeFeedProcessorInstance != null) {
                changeFeedProcessorInstance.stop().block();
            }

            logger.info("-->DELETE sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(500);

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("END Sample");
    }

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        ChangeFeedProcessorOptions cfOptions = new ChangeFeedProcessorOptions();
        cfOptions.setFeedPollDelay(Duration.ofMillis(100));
        cfOptions.setStartFromBeginning(true);
        return ChangeFeedProcessor.changeFeedProcessorBuilder()
            .setOptions(cfOptions)
            .setHostName(hostName)
            .setFeedContainer(feedContainer)
            .setLeaseContainer(leaseContainer)
            .setHandleChanges((List<JsonNode> docs) -> {
                for (JsonNode document : docs) {
                        //Duplicate each document update from the feed container into the materialized view container
                        updateInventoryTypeMaterializedView(document);
                }

            })
            .build();
    }

    private static void updateInventoryTypeMaterializedView(JsonNode document) {
        typeContainer.upsertItem(document).subscribe();
    }

    public static CosmosAsyncClient getCosmosClient() {

        return new CosmosClientBuilder()
                .setEndpoint(AccountSettings.HOST)
                .setKey(AccountSettings.MASTER_KEY)
                .setConnectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildAsyncClient();
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName) {
        return client.createDatabaseIfNotExists(databaseName).block().getDatabase();
    }

    public static void deleteDocument() {

        String jsonString =    "{\"id\" : \"" + idToDelete + "\""
                + ","
                + "\"brand\" : \"Jerry's\""
                + ","
                + "\"type\" : \"plums\""
                + ","
                + "\"quantity\" : \"50\""
                + ","
                + "\"ttl\" : 5"
                + "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode document = null;

        try {
            document = mapper.readTree(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }

        feedContainer.upsertItem(document,new CosmosItemRequestOptions()).block();
    }

    public static void deleteDatabase(CosmosAsyncDatabase cosmosDatabase) {
        cosmosDatabase.delete().block();
    }

    public static CosmosAsyncContainer createNewCollection(CosmosAsyncClient client, String databaseName, String collectionName, String partitionKey) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosAsyncContainerResponse containerResponse = null;

        try {
            containerResponse = collectionLink.read().block();

            if (containerResponse != null) {
                throw new IllegalArgumentException(String.format("Collection %s already exists in database %s.", collectionName, databaseName));
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, partitionKey);
        containerSettings.setDefaultTimeToLiveInSeconds(-1);
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        containerResponse = databaseLink.createContainer(containerSettings, 10000, requestOptions).block();

        if (containerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", collectionName, databaseName));
        }

        return containerResponse.getContainer();
    }

    public static CosmosAsyncContainer createNewLeaseCollection(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosAsyncContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();

            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

        leaseContainerResponse = databaseLink.createContainer(containerSettings, 400,requestOptions).block();

        if (leaseContainerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }

        return leaseContainerResponse.getContainer();
    }

    public static void createNewDocumentsJSON(CosmosAsyncContainer containerClient, Duration delay) {
        System.out.println("Creating documents\n");
        String suffix = RandomStringUtils.randomAlphabetic(10);
        List<String> brands = Arrays.asList("Jerry's","Baker's Ridge Farms","Exporters Inc.","WriteSmart","Stationary","L. Alfred","Haberford's","Drink-smart","Polaid","Choice Dairy");
        List<String> types = Arrays.asList("plums","ice cream","espresso","pens","stationery","cheese","cheese","kool-aid","water","milk");
        List<String> quantities = Arrays.asList("50","15","5","10","5","6","4","50","100","20");


        for (int i = 0; i < brands.size(); i++) {

            String id = UUID.randomUUID().toString();
            if (i==0) idToDelete=id;

            String jsonString =    "{\"id\" : \"" + id + "\""
                                 + ","
                                 + "\"brand\" : \"" + brands.get(i) + "\""
                                 + ","
                                 + "\"type\" : \"" + types.get(i) + "\""
                                 + ","
                                 + "\"quantity\" : \"" + quantities.get(i) + "\""
                                 + "}";

            ObjectMapper mapper = new ObjectMapper();
            JsonNode document = null;

            try {
                document = mapper.readTree(jsonString);
            } catch (Exception e) {
                e.printStackTrace();
            }

            containerClient.createItem(document).subscribe(doc -> {
                System.out.println(".\n");
            });

            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }
        }
    }

}
