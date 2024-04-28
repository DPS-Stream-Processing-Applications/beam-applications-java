package org.example;

import static io.undertow.UndertowOptions.ENABLE_HTTP2;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

public class UndertowMain {

  public static void main(String... args) {
    StatefulFunctionSpec spec =
        StatefulFunctionSpec.builder(GreeterFn.TYPE)
            .withValueSpec(GreeterFn.SEEN)
            .withSupplier(GreeterFn::new)
            .build();

    // obtain a request-reply handler based on the spec above
    StatefulFunctions functions = new StatefulFunctions();
    functions.withStatefulFunction(spec);
    RequestReplyHandler handler = functions.requestReplyHandler();

    // this is a generic HTTP server that hands off the request-body
    // to the handler above and visa versa.
    Undertow server =
        Undertow.builder()
            .addHttpListener(5000, "0.0.0.0")
            .setHandler(new UndertowHttpHandler(handler))
            .setServerOption(ENABLE_HTTP2, true)
            .build();

    server.start();
  }

  private static final class UndertowHttpHandler implements HttpHandler {

    private final RequestReplyHandler handler;

    UndertowHttpHandler(RequestReplyHandler handler) {
      this.handler = Objects.requireNonNull(handler);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
      exchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
    }

    private void onRequestBody(HttpServerExchange exchange, byte[] requestBytes) {
      exchange.dispatch();
      CompletableFuture<Slice> future = handler.handle(Slices.wrap(requestBytes));
      future.whenComplete((response, exception) -> onComplete(exchange, response, exception));
    }

    private void onComplete(HttpServerExchange exchange, Slice responseBytes, Throwable ex) {
      if (ex != null) {
        ex.printStackTrace(System.out);
        exchange.getResponseHeaders().put(Headers.STATUS, 500);
        exchange.endExchange();
        return;
      }
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
      exchange.getResponseSender().send(responseBytes.asReadOnlyByteBuffer());
    }
  }
}
