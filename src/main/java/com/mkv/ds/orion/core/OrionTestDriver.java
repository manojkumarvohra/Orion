package com.mkv.ds.orion.core;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrionTestDriver {

	private static Logger LOGGER = LogManager.getLogger(OrionTestDriver.class);

	private static final String APP_NAME = "Orion";

	public static void main(String[] args) {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.parquet").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME)
				.set("spark.sql.queryExecutionListeners", "com.mkv.ds.orion.core.listeners.OrionQueryInterceptor")
				.set("spark.orion.lineage.backend.handler", "NEO4J")
				.set("spark.orion.lineage.neo4j.backend.uri", "bolt://localhost:7687")
				.set("spark.orion.lineage.neo4j.backend.username", "neo4j")
				.set("spark.orion.lineage.neo4j.backend.password", "Welcome01@").setMaster("local[4,7]");

		SparkContext sc = SparkContext.getOrCreate(conf);

		SparkSession spark = SparkSession.builder().enableHiveSupport().config(sc.getConf()).getOrCreate();

		executeDummyQueries(spark);
	}

	private static void executeDummyQueries(SparkSession spark) {

		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "true")
				.load(OrionTestDriver.class.getResource("/ratings.csv").getPath());

		LOGGER.info("***CREATE DATABASE IF NOT EXISTS RAW");
		spark.sql("CREATE DATABASE IF NOT EXISTS RAW");

		LOGGER.info("***CREATE TEMP VIEW ratings_r_view");
		df.createOrReplaceTempView("ratings_r_view");

		LOGGER.info("***CREATE TABLE IF NOT EXISTS RAW.RAW_RATINGS LIKE ratings_r_view");
		spark.sql("CREATE TABLE IF NOT EXISTS RAW.RAW_RATINGS LIKE ratings_r_view");

		LOGGER.info("***INSERT OVERWRITE TABLE RAW.RAW_RATINGS SELECT * FROM ratings_r_view");
		spark.sql("INSERT OVERWRITE TABLE RAW.RAW_RATINGS SELECT * FROM ratings_r_view");

		LOGGER.info("***CREATE DATABASE IF NOT EXISTS PROCESSED");
		spark.sql("CREATE DATABASE IF NOT EXISTS PROCESSED");

		LOGGER.info("***CREATE TABLE IF NOT EXISTS PROCESSED.RATINGS LIKE ratings_r_view");
		spark.sql("CREATE TABLE IF NOT EXISTS PROCESSED.RATINGS LIKE ratings_r_view");

		LOGGER.info("***INSERT INTO TABLE PROCESSED.RATINGS SELECT * FROM RAW.RAW_RATINGS WHERE rating >=3 LIMIT 1");
		spark.sql("INSERT INTO TABLE PROCESSED.RATINGS SELECT * FROM RAW.RAW_RATINGS WHERE rating >=3 LIMIT 1");

		LOGGER.info("***CREATE TABLE IF NOT EXISTS PROCESSED.RATINGS_U (userId INT, movieId INT, rating INT)");
		spark.sql("CREATE TABLE IF NOT EXISTS PROCESSED.RATINGS_U (userId INT, movieId INT, rating INT)");

		LOGGER.info(
				"***INSERT INTO TABLE PROCESSED.RATINGS_U SELECT rr.userId, pr.movieId, rr.rating FROM RAW.RAW_RATINGS rr JOIN PROCESSED.RATINGS pr ON rr.userId=pr.userId");
		spark.sql(
				"INSERT INTO TABLE PROCESSED.RATINGS_U SELECT rr.userId, pr.movieId, rr.rating FROM RAW.RAW_RATINGS rr JOIN PROCESSED.RATINGS pr ON rr.userId=pr.userId");

		LOGGER.info("***CREATE VIEW VIEW_RATINGS AS SELECT * from PROCESSED.RATINGS_U where rating=3");
		spark.sql("CREATE VIEW IF NOT EXISTS VIEW_RATINGS AS SELECT * from PROCESSED.RATINGS_U where rating=3");

		LOGGER.info("***CREATE TABLE IF NOT EXISTS PROCESSED.LIMITED_RATINGS (userId INT, movieId INT, rating INT)");
		spark.sql("CREATE TABLE IF NOT EXISTS PROCESSED.LIMITED_RATINGS (userId INT, movieId INT, rating INT)");

		LOGGER.info("***INSERT INTO TABLE PROCESSED.LIMITED_RATINGS SELECT * FROM VIEW_RATINGS LIMIT 1");
		spark.sql("INSERT INTO TABLE PROCESSED.LIMITED_RATINGS SELECT * FROM VIEW_RATINGS LIMIT 1");

		LOGGER.info("***CREATE TABLE IF NOT EXISTS PROCESSED.COMPLEX_RATINGS (userId INT, movieId INT, rating INT)");
		spark.sql("CREATE TABLE IF NOT EXISTS PROCESSED.COMPLEX_RATINGS (userId INT, movieId INT, rating INT)");

		LOGGER.info(
				"***INSERT INTO TABLE PROCESSED.COMPLEX_RATINGS SELECT userId, movieId, rating FROM (SELECT lr.userId, lr.movieId, lr.rating FROM PROCESSED.LIMITED_RATINGS lr UNION SELECT orr.userId, orr.movieId, orr.rating FROM (SELECT rr.userId, pr.movieId, rr.rating FROM RAW.RAW_RATINGS rr JOIN PROCESSED.RATINGS pr ON rr.userId=pr.userId LIMIT 1) orr) LIMIT 1");
		spark.sql(
				"INSERT INTO TABLE PROCESSED.COMPLEX_RATINGS SELECT userId, movieId, rating FROM (SELECT lr.userId, lr.movieId, lr.rating FROM PROCESSED.LIMITED_RATINGS lr UNION SELECT orr.userId, orr.movieId, orr.rating FROM (SELECT rr.userId, pr.movieId, rr.rating FROM RAW.RAW_RATINGS rr JOIN PROCESSED.RATINGS pr ON rr.userId=pr.userId LIMIT 1) orr) LIMIT 1");

		LOGGER.info("DONE!");
	}
}