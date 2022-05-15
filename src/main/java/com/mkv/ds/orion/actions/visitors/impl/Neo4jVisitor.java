package com.mkv.ds.orion.actions.visitors.impl;

import static org.neo4j.driver.Values.parameters;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDatabaseCommand;
import org.apache.spark.sql.execution.command.CreateTableCommand;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.execution.command.CreateViewCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import com.mkv.ds.orion.model.Table;
import com.mkv.ds.orion.model.View;

import scala.Option;
import scala.collection.Iterator;

public class Neo4jVisitor extends AbstractEventsVisitor {

	private static Logger LOGGER = LogManager.getLogger(Neo4jVisitor.class);

	private final Driver driver;

	public Neo4jVisitor(String uri, String user, String password) {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}

	@Override
	public void visit(CreateDatabaseCommand createDbCommand) {

		String mergeQuery = "MERGE (d:DB {name: $name}) RETURN d.name";

		try (Session session = driver.session()) {
			String nodeCreationResult = session.writeTransaction(tx -> {
				Result result = tx.run(mergeQuery, parameters("name", createDbCommand.databaseName().toLowerCase()));
				return result.single().get(0).asString();
			});

			LOGGER.info("Create Database Result:" + nodeCreationResult);
		}
	}

	@Override
	public void visit(InsertIntoHiveTable insertIntoHiveTable) {

		TableIdentifier identifier = insertIntoHiveTable.table().identifier();

		Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

		List<Table> sourceTables = new ArrayList<>();

		List<View> sourceViews = new ArrayList<>();

		Iterator<LogicalPlan> children = insertIntoHiveTable.children().toList().iterator();

		getChildrenFromRelations(children, sourceTables, sourceViews);

		handleTableDbAssociation(target);

		for (Table source : sourceTables) {
			handleTableDbAssociation(source);
			associateTargetTableWithSourceTable(target, source);
		}

		for (View view : sourceViews) {
			handleViewDbAssociation(view);
			associateTargetTableWithSourceView(target, view);
		}
	}

	@Override
	public void visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRel) {

		Option<CatalogTable> catalogTable = insertIntoHadoopFsRel.catalogTable();

		if (!catalogTable.isEmpty()) {

			TableIdentifier identifier = catalogTable.get().identifier();

			Table target = new Table(identifier.database().get(), identifier.table());

			List<Table> sourceTables = new ArrayList<>();

			List<View> sourceViews = new ArrayList<>();

			Iterator<LogicalPlan> children = insertIntoHadoopFsRel.children().toList().iterator();

			getChildrenFromRelations(children, sourceTables, sourceViews);

			handleTableDbAssociation(target);

			for (Table source : sourceTables) {
				handleTableDbAssociation(source);
				associateTargetTableWithSourceTable(target, source);
			}

			for (View view : sourceViews) {
				handleViewDbAssociation(view);
				associateTargetTableWithSourceView(target, view);
			}
		}
	}

	@Override
	public void visit(CreateTableCommand createTableCommand) {

		TableIdentifier identifier = createTableCommand.table().identifier();

		Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

		handleTableDbAssociation(target);
	}

	@Override
	public void visit(CreateTableLikeCommand createTableLikeCommand) {

		TableIdentifier identifier = createTableLikeCommand.targetTable();

		Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

		handleTableDbAssociation(target);
	}

	@Override
	public void visit(CreateViewCommand createViewCommand) {

		TableIdentifier identifier = createViewCommand.name();

		Option<String> database = identifier.database();

		if (!database.isEmpty()) {

			View target = new View(database.get().toLowerCase(), identifier.table().toLowerCase());

			handleViewDbAssociation(target);
		}
	}

	private void associateTargetTableWithSourceTable(Table target, Table source) {

		try (Session session = driver.session()) {

			String targetSourceHandlingResult = session.writeTransaction(tx -> {

				String sourceTargetAssociation = "MATCH (t:TABLE {db: $t_dbName, name: $t_name}), (s:TABLE {db: $s_dbName, name: $s_name}) "
						+ " MERGE (t)-[r:READS_FROM]->(s) RETURN t.db, t.name, type(r), s.db, s.name";

				Result sourceTargetAssociationResult = tx.run(sourceTargetAssociation,
						parameters("t_dbName", target.getDatabase(), "t_name", target.getName(), "s_dbName",
								source.getDatabase(), "s_name", source.getName()));

				return sourceTargetAssociationResult.single().get(0).asString();
			});

			LOGGER.info("Target Source Association Result:" + targetSourceHandlingResult);
		}
	}

	private void associateTargetTableWithSourceView(Table target, View source) {

		try (Session session = driver.session()) {

			String targetSourceHandlingResult = session.writeTransaction(tx -> {

				String sourceTargetAssociation = "MATCH (t:TABLE {db: $t_dbName, name: $t_name}), (s:VIEW {db: $s_dbName, name: $s_name}) "
						+ " MERGE (t)-[r:READS_FROM]->(s) RETURN t.db, t.name, type(r), s.db, s.name";

				Result sourceTargetAssociationResult = tx.run(sourceTargetAssociation,
						parameters("t_dbName", target.getDatabase(), "t_name", target.getName(), "s_dbName",
								source.getDatabase(), "s_name", source.getName()));

				return sourceTargetAssociationResult.single().get(0).asString();
			});

			LOGGER.info("Target Source Association Result:" + targetSourceHandlingResult);
		}
	}

	private void handleTableDbAssociation(Table tbl) {

		try (Session session = driver.session()) {

			String targetHandlingResult = session.writeTransaction(tx -> {

				// CREATE target DB IF NOT EXISTS
				String dbMergeQuery = "MERGE (d:DB {name: $name}) RETURN d.name";
				tx.run(dbMergeQuery, parameters("name", tbl.getDatabase()));

				// CREATE target TABLE IF NOT EXISTS
				String tableMergeQuery = "MERGE (t:TABLE {db: $dbName, name: $name}) RETURN t.name";
				tx.run(tableMergeQuery, parameters("dbName", tbl.getDatabase(), "name", tbl.getName()));

				// Associate TABLE with DB IF NOT EXISTS
				String dbTableRelMergeQuery = "MATCH (db:DB {name: $dbName}), (t:TABLE {db: $dbName, name: $name}) MERGE (t)-[r:BELONGS_TO]->(db) RETURN t.name, type(r), db.dbName ";
				Result tblDbMergeRsult = tx.run(dbTableRelMergeQuery,
						parameters("dbName", tbl.getDatabase(), "name", tbl.getName()));

				return tblDbMergeRsult.single().get(0).asString();
			});

			LOGGER.info("Table DB Association Result:" + targetHandlingResult);
		}
	}

	private void handleViewDbAssociation(View view) {

		try (Session session = driver.session()) {

			String targetHandlingResult = session.writeTransaction(tx -> {

				// CREATE target DB IF NOT EXISTS
				String dbMergeQuery = "MERGE (d:DB {name: $name}) RETURN d.name";
				tx.run(dbMergeQuery, parameters("name", view.getDatabase()));

				// CREATE target VIEW IF NOT EXISTS
				String viewMergeQuery = "MERGE (v:VIEW {db: $dbName, name: $name}) RETURN v.name";
				tx.run(viewMergeQuery, parameters("dbName", view.getDatabase(), "name", view.getName()));

				// Associate VIEW with DB IF NOT EXISTS
				String dbViewRelMergeQuery = "MATCH (db:DB {name: $dbName}), (v:VIEW {db: $dbName, name: $name}) MERGE (v)-[r:BELONGS_TO]->(db) RETURN v.name, type(r), db.dbName";
				Result viewDbMergeRsult = tx.run(dbViewRelMergeQuery,
						parameters("dbName", view.getDatabase(), "name", view.getName()));

				return viewDbMergeRsult.single().get(0).asString();
			});

			LOGGER.info("Table DB Association Result:" + targetHandlingResult);
		}
	}
}