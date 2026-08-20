/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.example;

import static java.lang.System.Logger.Level.INFO;

import org.apache.airflow.sdk.*;

@Builder.Dag(id = "java_xcom_casting_example")
public class XComCastingExample {
  private static final System.Logger log = System.getLogger(XComCastingExample.class.getName());

  @Builder.Task(id = "produce_number")
  public int produceNumber() {
    log.log(INFO, "Producing int 7");
    return 7;
  }

  // Any primitive numeric type (byte, short, int, long, float, double) and its boxed form works the same way.
  @Builder.Task(id = "widen_to_long")
  public long widenToLong(@Builder.XCom(task = "produce_number") long value) {
    log.log(INFO, "Got long {0}", value);
    return value + 1;
  }

  @Builder.Task(id = "widen_to_double")
  public void widenToDouble(@Builder.XCom(task = "widen_to_long") double value) {
    log.log(INFO, "Got double {0}", value);
    if (value != 8.0) {
      throw new RuntimeException("expected 8.0 but got " + value);
    }
  }

  @Builder.Task(id = "produce_nothing")
  public void produceNothing() {
    // Pushes no return_value XCom.
  }

  @Builder.Task(id = "consume_nullable")
  public void consumeNullable(@Builder.XCom(task = "produce_nothing") Integer value) {
    log.log(INFO, "Got nullable int {0}", value);
    if (value != null) {
      throw new RuntimeException("expected null but got " + value);
    }
  }

  @Builder.Task(id = "produce_fraction")
  public double produceFraction() {
    log.log(INFO, "Producing double 1.5");
    return 1.5;
  }

  @Builder.Task(id = "consume_float")
  public void consumeFloat(@Builder.XCom(task = "produce_fraction") float value) {
    log.log(INFO, "Got float {0}", value);
    if (value != 1.5f) {
      throw new RuntimeException("expected 1.5 but got " + value);
    }
  }
}
