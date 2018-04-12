#  Copyright 2016-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Amazon Software License (the "License"). You may not use
#  this file except in compliance with the License. A copy of the License is
#  located at:
#
#    http://aws.amazon.com/asl/
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_input_data_path'])

# input data
medicare_input = args['s3_input_data_path']


# s3 output directories
medicare_cast = "s3://my-includes/glue-datasets/examples/output-dir/medicare_json_cast"
medicare_project = "s3://my-includes/glue-datasets/examples/output-dir/medicare_json_project"
medicare_cols = "s3://my-includes/glue-datasets/examples/output-dir/medicare_json_make_cols"
medicare_struct = "s3://my-includes/glue-datasets/examples/output-dir/medicare_json_make_struct"
medicare_sql = "s3://my-includes/glue-datasets/examples/output-dir/medicare_json_sql"

# Read data into a dynamic frame
raw_df = spark.read.option("header", "true").csv(medicare_input)
medicare_dyf = DynamicFrame.fromDF(
    raw_df,
    glueContext,
    "medicare_dyf"
)

medicare_dyf.printSchema()

# The `provider id` field will be choice between long and string

# Cast choices into integers, those values that cannot cast result in null
medicare_res_cast = medicare_dyf.resolveChoice(specs = [('Provider Id','cast:long')])
medicare_res_project = medicare_dyf.resolveChoice(specs = [('Provider Id','project:long')])
medicare_res_make_cols = medicare_dyf.resolveChoice(specs = [('Provider Id','make_cols')])
medicare_res_make_struct = medicare_dyf.resolveChoice(specs = [('Provider Id','make_struct')])

# Spark SQL on a Spark dataframe
medicare_df = medicare_dyf.toDF()
medicare_df.createOrReplaceTempView("medicareTable")
medicare_sql_df = spark.sql("SELECT * FROM medicareTable WHERE 'Total Discharges ' > 30")
medicare_sql_dyf = DynamicFrame.fromDF(medicare_sql_df, glueContext, "medicare_sql_dyf")

# Write it out in Json
glueContext.write_dynamic_frame.from_options(frame = medicare_res_cast, connection_type = "s3", connection_options = {"path": medicare_cast}, format = "json")
glueContext.write_dynamic_frame.from_options(frame = medicare_res_project, connection_type = "s3", connection_options = {"path": medicare_project}, format = "json")
glueContext.write_dynamic_frame.from_options(frame = medicare_res_make_cols, connection_type = "s3", connection_options = {"path": medicare_cols}, format = "json")
glueContext.write_dynamic_frame.from_options(frame = medicare_res_make_struct, connection_type = "s3", connection_options = {"path": medicare_struct}, format = "json")
glueContext.write_dynamic_frame.from_options(frame = medicare_sql_dyf, connection_type = "s3", connection_options = {"path": medicare_sql}, format = "json")
