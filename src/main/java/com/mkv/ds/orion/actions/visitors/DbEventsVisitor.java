package com.mkv.ds.orion.actions.visitors;

import org.apache.spark.sql.execution.command.CreateDatabaseCommand;
import org.apache.spark.sql.execution.command.CreateTableCommand;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.execution.command.CreateViewCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;

public interface DbEventsVisitor {

	public void visit(CreateDatabaseCommand createDbCommand);

	public void visit(InsertIntoHiveTable insertIntoHiveTable);

	public void visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRelationCommand);

	public void visit(CreateTableCommand createTableCommand);
	
	public void visit(CreateTableLikeCommand createTableLikeCommand);

	public void visit(CreateViewCommand createViewCommand);

}
