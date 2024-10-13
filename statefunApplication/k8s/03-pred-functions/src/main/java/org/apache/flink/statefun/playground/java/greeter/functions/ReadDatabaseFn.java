package org.apache.flink.statefun.playground.java.greeter.functions;

/*
public class ReadDatabaseFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/readDatabase");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(LinearRegressionFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/decisionTree");
    static final TypeName INBOX_2 = TypeName.typeNameFromString("pred/linearRegression");
    static final TypeName INBOX_3 = TypeName.typeNameFromString("pred/average");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        return context.done();
    }
}
 */