package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.concurrent.CompletableFuture;

public class SinkTrainFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/sinkTrain");


    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(SinkTrainFn::new)
                    .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        return context.done();
    }
}
