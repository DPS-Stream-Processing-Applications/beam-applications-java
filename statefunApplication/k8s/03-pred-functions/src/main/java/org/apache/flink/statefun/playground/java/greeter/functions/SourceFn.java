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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.statefun.playground.java.greeter.types.generated.SourceEntry;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.SOURCE_ENTRY_PROTOBUF_TYPE;


public final class SourceFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/source");
    static final TypeName INBOX = TypeName.typeNameFromString("pred/senmlParse");


    private static final ValueSpec<Long> MSGID_COUNT = ValueSpec
            .named("message_counter")
            .withLongType();

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpec(MSGID_COUNT)
                    .withSupplier(SourceFn::new)
                    .build();
    private Logger l;

    public void initLogger(Logger l_) {
        this.l = l_;
    }


    private long extractTimeStamp(String row) {
        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(row, JsonArray.class);

        String pickupDatetime = null;
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
            if (jsonObject.has("n") && jsonObject.get("n").getAsString().equals("pickup_datetime")) {
                pickupDatetime = jsonObject.get("vs").getAsString();
                break;
            }
        }
        jsonArray = null;
        gson = null;
        return convertToUnixTimeStamp(pickupDatetime);
    }

    private long convertToUnixTimeStamp(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        long unixTimestampSeconds;
        try {
            Date date = dateFormat.parse(dateString);
            long unixTimestamp = date.getTime();
            unixTimestampSeconds = unixTimestamp / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return unixTimestampSeconds;
    }

    public void setup() {
        initLogger(LoggerFactory.getLogger("APP"));
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        String rowString = new String(message.rawValue().toByteArray(), StandardCharsets.UTF_8);
        String datasetType = System.getenv("DATASET");
        if (datasetType == null) {
            throw new RuntimeException("Dataset is null");
        }
        String newRow;
        try {
            if (datasetType.equals("TAXI")) {
                newRow = "{\"e\":" + rowString + ",\"bt\":" + extractTimeStamp(rowString) + "}";
            } else if (datasetType.equals("FIT")) {
                newRow = "{\"e\":" + rowString + ",\"bt\": \"1358101800000\"}";

            } else {
                newRow = "{\"e\":" + rowString + ",\"bt\":1358101800000}";
            }

            long msgId = context.storage().get(MSGID_COUNT).orElse(1L);
            final SourceEntry sourceEntry =
                    SourceEntry.newBuilder()
                            .setMsgid(msgId)
                            .setPayload(newRow)
                            .setDataSetType(datasetType).build();


            msgId += 1;
            context.storage().set(MSGID_COUNT, msgId);

            /*
            if (msgId % 100 == 0) {
                sourceEntry.setArrivalTime(System.currentTimeMillis());
            }
             */

            System.out.println("Source msgid: "+sourceEntry.getMsgid());
            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(sourceEntry.getMsgid()))
                            .withCustomType(SOURCE_ENTRY_PROTOBUF_TYPE, sourceEntry)
                            .build());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return context.done();
    }
}
