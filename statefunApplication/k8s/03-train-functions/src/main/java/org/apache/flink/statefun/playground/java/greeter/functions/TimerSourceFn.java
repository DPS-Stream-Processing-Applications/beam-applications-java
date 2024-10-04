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

import org.apache.flink.statefun.playground.java.greeter.types.SourceEntry;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.Source_ENTRY_JSON_TYPE;


public final class TimerSourceFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/source");
    static final TypeName INBOX = TypeName.typeNameFromString("pred/tableRead");
    private static final ValueSpec<Long> MSGID_COUNT = ValueSpec
            .named("message_counter")
            .withLongType();
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpec(MSGID_COUNT)
                    .withSupplier(TimerSourceFn::new)
                    .build();
    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        String rowString = new String(message.rawValue().toByteArray(), StandardCharsets.UTF_8);
        String ROWKEYSTART = rowString.split(",")[1];
        String ROWKEYEND = rowString.split(",")[2];
        SourceEntry values = new SourceEntry();
        values.setRowString(rowString);
        long msgId = context.storage().get(MSGID_COUNT).orElse(1L);
        values.setMsgid(Long.toString(msgId));
        msgId += 1;
        context.storage().set(MSGID_COUNT, msgId);
        values.setArrivalTime(System.currentTimeMillis());
        values.setRowKeyStart(ROWKEYSTART);
        values.setRowKeyEnd(ROWKEYEND);
        values.setDataSetType(System.getenv("DATASET"));
        values.setArrivalTime(System.currentTimeMillis());
        context.send(
                MessageBuilder.forAddress(INBOX, String.valueOf(values.getMsgid()))
                        .withCustomType(Source_ENTRY_JSON_TYPE, values)
                        .build());
        return context.done();
    }
}
