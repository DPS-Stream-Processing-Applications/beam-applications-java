/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.AzureBlobDownloadTask;
import org.apache.flink.statefun.playground.java.greeter.types.BlobReadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.MqttSubscribeEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.BLOB_READ_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.MQTT_SUBSCRIBE_ENTRY_JSON_TYPE;

/**
 * A simple function that computes personalized greetings messages based on a given.
 * Then, it sends the greetings message back to the user via an egress Kafka topic.
 */
public final class BlobReadFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/blobRead");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME).withSupplier(BlobReadFn::new).build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/decisionTree");
    static final TypeName INBOX_2 = TypeName.typeNameFromString("pred/linearRegression");

    public  int calculatePrimes(int limit) {
        int count = 0;
        for (int i = 2; i <= limit; i++) {
            if (isPrime(i)) {
                count++;
            }
        }
        return count;
    }

    public  boolean isPrime(int number) {
        if (number <= 1) {
            return false;
        }
        for (int i = 2; i * i <= number; i++) {
            if (number % i == 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        try {
            MqttSubscribeEntry mqttSubscribeEntry = message.as(MQTT_SUBSCRIBE_ENTRY_JSON_TYPE);
            String BlobModelPath = mqttSubscribeEntry.getBlobModelPath();
            String analyticsType = mqttSubscribeEntry.getAnalaytictype();

            String msgId = mqttSubscribeEntry.getMsgid();
            calculatePrimes(1000);

            byte[] BlobModelObject = null;
            BlobReadEntry blobReadEntry = new BlobReadEntry(BlobModelObject, msgId, "modelupdate", analyticsType, "meta", mqttSubscribeEntry.getDataSetType());
            //blobReadEntry.setArrivalTime(mqttSubscribeEntry.getArrivalTime());
            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(blobReadEntry.getMsgid()))
                            .withCustomType(BLOB_READ_ENTRY_JSON_TYPE, blobReadEntry)
                            .build());

            context.send(
                    MessageBuilder.forAddress(INBOX_2, String.valueOf(blobReadEntry.getMsgid()))
                            .withCustomType(BLOB_READ_ENTRY_JSON_TYPE, blobReadEntry)
                            .build());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return context.done();
    }

}
