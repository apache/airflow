 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Guide for Apache Pinot Operators
=================================
 

`Apache Pinot <https://pinot.apache.org/>`__ is a column-oriented, open-source,
distributed data store written in Java. Pinot is designed to execute OLAP queries
with low latency. It is suited in contexts where fast analytics,such as aggregations,
are needed on immutable data, possibly, with real-time data ingestion.

.. _howto/operator:PinotCoreOperators.html:


Transform Operator
==================
The transform operator modifies your input records, or transfers them unchanged, 
guided by the logic of the transformation expression you supply.

Using the transform operator
============================
``https://www.tabnine.com/code/java/packages/org.apache.pinot.core.operator.transform``

The `TransformOperator` is the constructor for the class.

`AdditionTransformFunction.init(...)`

@Override
public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
 // Check that there are more than 1 arguments
 if (arguments.size() < 2) {
  throw new IllegalArgumentException("At least 2 arguments are required for ADD transform function");
 }
 for (``TransformFunction`` argument : arguments) {
  if (argument instanceof LiteralTransformFunction) {
   _literalSum += Double.parseDouble(((LiteralTransformFunction) argument).getLiteral());
  } else {
   if (!argument.getResultMetadata().isSingleValue()) {
    throw new IllegalArgumentException("All the arguments of ADD transform function must be single-valued");
   }
   _transformFunctions.add(argument);
  }
 }
}


Using the DocIdSetOperator
============================
``https://www.tabnine.com/code/java/methods/org.apache.pinot.core.operator.DocIdSetOperator/%3Cinit%3E``
`DocIdSetPlanNode.run()`

@Override
public ``DocIdSetOperator`` run() {
 return new ``DocIdSetOperator``(_filterPlanNode.run(), _maxDocPerCall);
}

Using the NextBlockOperator
============================
``https://www.tabnine.com/code/java/methods/org.apache.pinot.core.operator.ProjectionOperator/nextBlock``
   `.getNextBlock()method`
   
@Override
protected TransformBlock ``getNextBlock``() {
 ProjectionBlock projectionBlock = _projectionOperator.nextBlock();
 if (projectionBlock == null) {
  return null;
 } else {
  return new TransformBlock(projectionBlock, _transformFunctionMap);
 }
}


Using the ProjectionOperator
============================
``https://www.tabnine.com/code/java/classes/org.apache.pinot.core.operator.ProjectionOperator``
   `StarTreeProjectionPlanNode.run()`

@Override
public ProjectionOperator run() {
 return new ``ProjectionOperator``(_dataSourceMap, _starTreeDocIdSetPlanNode.run());
}


Using the TransformFunctionFactory Operator
===========================================
   ``https://www.tabnine.com/code/java/classes/org.apache.pinot.core.operator.transform.function.TransformFunctionFactory``
   `TransformFunctionFactory.get()`

/**
 * Constructor for the class
 *
 * @param projectionOperator Projection operator
 * @param expressions Set of expressions to evaluate
 */
public TransformOperator(@Nonnull ProjectionOperator projectionOperator,
  @Nonnull Set<TransformExpressionTree> expressions) {
 _projectionOperator = projectionOperator;
 _dataSourceMap = projectionOperator.getDataSourceMap();
 for (TransformExpressionTree expression : expressions) {
  TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
  _transformFunctionMap.put(expression, transformFunction);
 }
}

Using the getNumeEntriesScannedInFilter Operator
===========================================

   ``https://www.tabnine.com/code/java/methods/org.apache.pinot.core.operator.ExecutionStatistics/getNumEntriesScannedInFilter``

   `QueriesTestUtils.testInnerSegmentExecutionStatistics(...)`

public static void testInnerSegmentExecutionStatistics(ExecutionStatistics executionStatistics,
  long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
  long expectedNumTotalRawDocs) {
 Assert.assertEquals(executionStatistics.getNumDocsScanned(), expectedNumDocsScanned);
 Assert.assertEquals(executionStatistics.``getNumEntriesScannedInFilter()``, expectedNumEntriesScannedInFilter);
 Assert.assertEquals(executionStatistics.``getNumEntriesScannedPostFilter()``, expectedNumEntriesScannedPostFilter);
 Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), expectedNumTotalRawDocs);
}


For further information look at: 
* `https://www.tabnine.com/code/query/%22org.apache.pinot.core.operator%22`
* 

