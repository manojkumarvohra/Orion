package com.mkv.ds.orion.actions.visitors;

import static com.mkv.ds.orion.constants.Constants.BACKEND_SPARK_CONF;
import static com.mkv.ds.orion.constants.Constants.NEO4J_PASSWORD_SPARK_CONF;
import static com.mkv.ds.orion.constants.Constants.NEO4J_URI_SPARK_CONF;
import static com.mkv.ds.orion.constants.Constants.NEO4J_USERNAME_SPARK_CONF;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;

import com.mkv.ds.orion.actions.visitors.impl.ConsoleVisitor;
import com.mkv.ds.orion.actions.visitors.impl.Neo4jVisitor;

public class VisitorFactory {

	public static DbEventsVisitor getVisistorFor(SparkConf conf) {

		DbEventsVisitor visitor = null;

		BackendHandlerType backendType = BackendHandlerType
				.valueOf(conf.get(BACKEND_SPARK_CONF, "console").trim().toUpperCase());

		switch (backendType) {

		case CONSOLE:
			visitor = getConsoleBackendHandler();

		case NEO4J:
			visitor = getNeo4jBackendHandler(conf);

		default:
			break;
		}

		return visitor;
	}

	private static DbEventsVisitor getNeo4jBackendHandler(SparkConf conf) {

		String uri = conf.get(NEO4J_URI_SPARK_CONF);
		String userName = conf.get(NEO4J_USERNAME_SPARK_CONF);
		String password = conf.get(NEO4J_PASSWORD_SPARK_CONF);

		if (StringUtils.isBlank(uri) || StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
			throw new RuntimeException("Invalid Neo4J Backend Handler Configuration!");
		}

		return new Neo4jVisitor(uri, userName, password);
	}

	private static DbEventsVisitor getConsoleBackendHandler() {
		return new ConsoleVisitor();
	}
}