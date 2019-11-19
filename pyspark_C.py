from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import re

class MyConsumer:

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Reciever") \
            .getOrCreate()

        # Define schema of json
        self.schema = StructType() \
                .add("id", StringType()) \
                .add("first_name", StringType()) \
                .add("last_name", StringType()) \
                .add("email", StringType()) \
                .add("gender", StringType()) \
                .add("ip_address", StringType()) \
                .add("date", StringType()) \
                .add("country", StringType())

    def log_silencer(self):
        log4j = self.spark._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    def schema_loader(self):
        # load data into spark-structured streaming
        self.df = self.spark \
                      .readStream \
                      .format("kafka") \
                      .option("kafka.bootstrap.servers", "localhost:9092") \
                      .option("startingOffsets","earliest") \
                      .option("subscribe", "json-topic") \
                      .option('includeTimestamp', 'true')\
                      .load() 

    #calculating total recieved messages for verification
    def total_messages_recieved_calculator(self):
        self.stream = self.df.selectExpr("topic").agg(count("topic")).alias("count")



    def calculations(self):

        @udf(returnType=StringType())
        def check(pp):
            regex = '(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])' 
            if(re.search(regex, pp)):  
                return pp

        self.total_messages_recieved_calculator()
        all_json_values=self.df.select(from_json(col("value").cast("string"), self.schema).alias("parsed_value"),"timestamp").select("parsed_value.*","timestamp")
        change_id_to_int=all_json_values.withColumn("id",coalesce(col("id").cast("int"),lit(0)))
        change_date_format=change_id_to_int.withColumn("date",coalesce(to_date(to_timestamp("date","dd/MM/yyyy"),'dd-MM-yyyy'),to_date(lit(0).cast("timestamp"),'dd-MM-yyyy')))
        #capitalizing first name or putting zero if NULL
        capitalize_first_letter_firstname_or_put_zero=change_date_format.withColumn("first_name",coalesce(initcap(col("first_name")),lit(0).cast("string")))
        #checking ip address or putting zero if NULL
        correct_ip_address_or_put_zero=capitalize_first_letter_firstname_or_put_zero.withColumn("ip_address",coalesce(check(col("ip_address")),lit(0).cast("string")))
        #calculating unique users based on email
        approx_distinct=correct_ip_address_or_put_zero.withWatermark("timestamp", "2 milliseconds").groupby(window(col("timestamp"), "2 milliseconds","2 milliseconds")).agg(approx_count_distinct("email"))
        #calculating simple aggregations
        aggregate_window_country=correct_ip_address_or_put_zero.withWatermark("timestamp", "10 milliseconds").groupby(window(col("timestamp"), "10 milliseconds","10 milliseconds"),"country").agg(count("country"))

        
        query0 = self.stream \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        query1_1 = aggregate_window_country \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        query1_2 = correct_ip_address_or_put_zero \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query2 = approx_distinct \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query3 = aggregate_window_country \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        query0.awaitTermination()
        query1_1.awaitTermination()
        query1_2.awaitTermination()
        query2.awaitTermination()
        query3.awaitTermination()

Streaming=MyConsumer()
Streaming.log_silencer()
Streaming.schema_loader()
Streaming.calculations()
