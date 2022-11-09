#!/usr/bin/r
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
Sys.setenv(SPARK_HOME = "/home/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session()
# Create the SparkDataFrame
df <- as.DataFrame(faithful)
head(summarize(groupBy(df, df$waiting), count = n(df$waiting)))
