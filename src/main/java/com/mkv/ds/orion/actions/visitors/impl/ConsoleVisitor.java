package com.mkv.ds.orion.actions.visitors.impl;

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

import com.mkv.ds.orion.actions.CreateDatabase;
import com.mkv.ds.orion.actions.CreateTable;
import com.mkv.ds.orion.actions.CreateView;
import com.mkv.ds.orion.actions.DatabaseEvent;
import com.mkv.ds.orion.actions.LoadTable;
import com.mkv.ds.orion.model.Database;
import com.mkv.ds.orion.model.Table;
import com.mkv.ds.orion.model.View;

import scala.Option;
import scala.collection.Iterator;

public class ConsoleVisitor extends AbstractEventsVisitor {

	private static Logger LOGGER = LogManager.getLogger(ConsoleVisitor.class);

	@Override
	public void visit(CreateDatabaseCommand createDbCommand) {

		DatabaseEvent event = new CreateDatabase(new Database(createDbCommand.databaseName()));

		LOGGER.info(event);
	}

	@Override
	public void visit(InsertIntoHiveTable insertIntoHiveTable) {

		TableIdentifier identifier = insertIntoHiveTable.table().identifier();

		Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

		List<Table> sourceTables = new ArrayList<>();

		List<View> sourceViews = new ArrayList<>();

		Iterator<LogicalPlan> children = insertIntoHiveTable.children().toList().iterator();

		getChildrenFromRelations(children, sourceTables, sourceViews);

		DatabaseEvent event = new LoadTable(target, sourceTables, sourceViews);

		LOGGER.info(event);
	}

	@Override
	public void visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRel) {

		Option<CatalogTable> catalogTable = insertIntoHadoopFsRel.catalogTable();

		if (!catalogTable.isEmpty()) {

			TableIdentifier identifier = catalogTable.get().identifier();

			Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

			List<Table> sourceTables = new ArrayList<>();

			List<View> sourceViews = new ArrayList<>();

			Iterator<LogicalPlan> children = insertIntoHadoopFsRel.children().toList().iterator();

			getChildrenFromRelations(children, sourceTables, sourceViews);

			LoadTable event = new LoadTable(target, sourceTables, sourceViews);

			LOGGER.info(event);
		}
	}

	@Override
	public void visit(CreateTableCommand createTableCommand) {

		TableIdentifier identifier = createTableCommand.table().identifier();

		Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

		CreateTable event = new CreateTable(target);

		LOGGER.info(event);
	}

	@Override
	public void visit(CreateTableLikeCommand createTableLikeCommand) {

		TableIdentifier identifier = createTableLikeCommand.targetTable();

		Table target = new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase());

		CreateTable event = new CreateTable(target);

		LOGGER.info(event);
	}

	@Override
	public void visit(CreateViewCommand createViewCommand) {

		TableIdentifier identifier = createViewCommand.name();

		Option<String> database = identifier.database();

		if (!database.isEmpty()) {

			View target = new View(database.get().toLowerCase(), identifier.table().toLowerCase());

			CreateView event = new CreateView(target);

			LOGGER.info(event);
		}
	}
}