import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.sdk.io.*;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public class ExampleModule implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        binder.bindFunctionProvider(ExampleFunction.TYPE, unused -> new ExampleFunction());
    }

    public static final class ExampleFunction implements StatefulFunction {
        public static final TypeName TYPE = TypeName.typeNameOf("example", "hello");

        @Override
        public void invoke(Context context, Object input) {
            String arg = (String) input;
            System.out.println("Hello from java " + context.self().id() + ": you wrote " + arg + "!");
        }
    }

    public static void main(String[] args) throws Exception {
        StatefulFunctions statefulFunctions = new StatefulFunctions();
        statefulFunctions.withStatefulFunctionModule("example", new ExampleModule());

        StatefulFunctionsUniverse statefulFunctionsUniverse = new StatefulFunctionsUniverse();
        statefulFunctionsUniverse.addService(new ExampleService(statefulFunctions));

        statefulFunctionsUniverse.start();
    }

    public static final class ExampleService extends FunctionTypeRouter {
        private static final long serialVersionUID = 1L;

        ExampleService(StatefulFunctions functions) {
            super(functions);
        }

        @Override
        public void configure(Map<String, String> globalConfiguration, Router router) {
            router.route(ExampleFunction.TYPE)
                    .to(new FunctionProvider<>(ExampleFunction.TYPE, unused -> new ExampleFunction()));
        }
    }
}
