package org.example;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

public class GreeterFn implements StatefulFunction {

  static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/greeter");

  static final TypeName INBOX = TypeName.typeNameFromString("com.example.fns/inbox");

  static final ValueSpec<Integer> SEEN = ValueSpec.named("seen").withIntType();

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    String name = message.asUtf8String();

    var storage = context.storage();
    var seen = storage.get(SEEN).orElse(0);
    storage.set(SEEN, seen + 1);

    context.send(
        MessageBuilder.forAddress(INBOX, name)
            .withValue("Hello " + name + " for the " + seen + "th time!")
            .build());

    return context.done();
  }
}
