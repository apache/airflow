package org.apache.airflow.example;

import org.apache.airflow.sdk.*;
import org.jetbrains.annotations.NotNull;
import java.util.List;

public class ExampleBundleBuilder implements BundleBuilder {
  @NotNull
  @Override
  public Iterable<Dag> getDags() {
    return List.of(JavaExampleBuilder.build());
  }

  public static void main(String[] args) {
    var bundle = new ExampleBundleBuilder().build();
    Server.create(args).serve(bundle);
  }
}
