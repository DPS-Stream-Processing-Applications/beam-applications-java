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
import io.undertow.UndertowOptions;
import org.apache.flink.statefun.playground.java.greeter.functions.*;
import org.apache.flink.statefun.playground.java.greeter.tasks.WriteToDatabase;
import org.apache.flink.statefun.playground.java.greeter.undertow.UndertowHttpHandler;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

import java.io.IOException;

/**
 * Entry point to start an {@link Undertow} web server that exposes the functions that build up our
 * User Greeter application, {@link SourceFn} and {@link BlobReadFn}.
 *
 * <p>Here we are using the {@link Undertow} web server just to show a very simple demonstration.
 * You may choose any web server that can handle HTTP request and responses, for example, Spring,
 * Micronaut, or even deploy your functions on popular FaaS platforms, like AWS Lambda.
 */
public final class GreeterAppServer {


    public static void main(String[] args) throws IOException {
        final StatefulFunctions functions = new StatefulFunctions();
        functions.withStatefulFunction(SourceFn.SPEC);
        functions.withStatefulFunction(BlobReadFn.SPEC);
        functions.withStatefulFunction(MqttSubscribeFn.SPEC);
        functions.withStatefulFunction(AverageFn.SPEC);
        functions.withStatefulFunction(DecisionTreeFn.SPEC);
        functions.withStatefulFunction(ErrorEstimateFn.SPEC);
        functions.withStatefulFunction(LinearRegressionFn.SPEC);
        functions.withStatefulFunction(ParsePredictFn.SPEC);
        functions.withStatefulFunction(ReadDatabaseFn.SPEC);
        functions.withStatefulFunction(SinkFn.SPEC);
        functions.withStatefulFunction(MqttPublishFn.SPEC);

        System.out.println(System.getenv("MONGO_DB_ADDRESS"));
        String ipAddress = System.getenv("MONGO_DB_ADDRESS");
        WriteToDatabase writeToDatabase = new WriteToDatabase(ipAddress, "mydb");
        writeToDatabase.prepareDataBaseForApplication();


        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();
        final Undertow httpServer =
                Undertow.builder()
                        .addHttpListener(8000, "0.0.0.0")
                        .setHandler(new UndertowHttpHandler(requestReplyHandler))
                        .setWorkerThreads(1000)
                        .setServerOption(UndertowOptions.NO_REQUEST_TIMEOUT, 100 * 1000)   // 60 seconds no request timeout
                        .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                        .build();
        try {
            httpServer.start();
        }
        catch (Exception e){
            System.out.println(e);
            throw new RuntimeException("Server start: "+e);
        }
    }
}
