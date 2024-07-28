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

package org.apache.flink.statefun.playground.java.greeter;

import io.undertow.Undertow;
import org.apache.flink.statefun.playground.java.greeter.functions.*;
import org.apache.flink.statefun.playground.java.greeter.tasks.WriteToDatabase;
import org.apache.flink.statefun.playground.java.greeter.undertow.UndertowHttpHandler;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

import java.io.IOException;


public final class GreeterAppServer {


    public static void main(String[] args) throws IOException {
        final StatefulFunctions functions = new StatefulFunctions();
        functions.withStatefulFunction(TimerSourceFn.SPEC);
        functions.withStatefulFunction(AnnotateFn.SPEC);
        functions.withStatefulFunction(BlobWriteFn.SPEC);
        functions.withStatefulFunction(DecisionTreeTrainFn.SPEC);
        functions.withStatefulFunction(LinearRegressionTrainFn.SPEC);
        functions.withStatefulFunction(MqttPublishTrainFn.SPEC);
        functions.withStatefulFunction(TableReadFn.SPEC);
        functions.withStatefulFunction(SinkTrainFn.SPEC);

        String ipAddress = System.getenv("MONGO_DB_ADDRESS");
        WriteToDatabase writeToDatabase = new WriteToDatabase(ipAddress, "mydb");
        writeToDatabase.prepareDataBaseForApplication();
        System.out.println("TRAIN-"+System.getenv("DATASET"));

        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();
        final Undertow httpServer =
                Undertow.builder()
                        .addHttpListener(8000, "0.0.0.0")
                        .setHandler(new UndertowHttpHandler(requestReplyHandler))
                        .build();
        httpServer.start();
    }
}
