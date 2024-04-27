package org.example;

import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJob;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    StatefulFunctionsConfig stateFunConfig = StatefulFunctionsConfig.fromEnvironment(env);

    stateFunConfig.setProvider(
        (StatefulFunctionsUniverseProvider)
            (classLoader, statefulFunctionsConfig) -> {
              Modules modules = Modules.loadFromClassPath();
              return modules.createStatefulFunctionsUniverse(stateFunConfig);
            });

    StatefulFunctionsJob.main(env, stateFunConfig);
  }
}
