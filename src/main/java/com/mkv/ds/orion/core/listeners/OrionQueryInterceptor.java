package com.mkv.ds.orion.core.listeners;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.command.CreateDatabaseCommand;
import org.apache.spark.sql.execution.command.CreateTableCommand;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.execution.command.CreateViewCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.apache.spark.sql.util.QueryExecutionListener;

import com.mkv.ds.orion.actions.visitors.DbEventsVisitor;
import com.mkv.ds.orion.actions.visitors.VisitorFactory;

public class OrionQueryInterceptor implements QueryExecutionListener {

	private boolean initialized;

	private DbEventsVisitor visitor;

	@Override
	public void onFailure(String funcName, QueryExecution qe, Exception ex) {
		// NO-OP
	}

	@Override
	public void onSuccess(String funcName, QueryExecution qe, long durationNs) {

		// first check: prevent threads from entering into synchronized block always
		if (!this.initialized) {
			synchronized (this) {
				// second check: if two or more threads find listener context uninitialized at
				// same time on previous IF check.
				if (!this.initialized) {
					this.visitor = VisitorFactory.getVisistorFor(qe.sparkSession().sparkContext().getConf());
					this.initialized = true;
				}
			}
		}

		LogicalPlan logicalPlan = qe.analyzed();

		if (logicalPlan instanceof CreateDatabaseCommand) {
			CreateDatabaseCommand createDb = (CreateDatabaseCommand) logicalPlan;
			visitor.visit(createDb);
		} else if (logicalPlan instanceof InsertIntoHiveTable) {
			InsertIntoHiveTable insertIntoHiveTable = (InsertIntoHiveTable) logicalPlan;
			visitor.visit(insertIntoHiveTable);
		} else if (logicalPlan instanceof InsertIntoHadoopFsRelationCommand) {
			InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRel = (InsertIntoHadoopFsRelationCommand) logicalPlan;
			visitor.visit(insertIntoHadoopFsRel);
		} else if (logicalPlan instanceof CreateTableCommand) {
			CreateTableCommand createTable = (CreateTableCommand) logicalPlan;
			visitor.visit(createTable);
		} else if (logicalPlan instanceof CreateTableLikeCommand) {
			CreateTableLikeCommand createTableLike = (CreateTableLikeCommand) logicalPlan;
			visitor.visit(createTableLike);
		} else if (logicalPlan instanceof CreateViewCommand) {
			CreateViewCommand createView = (CreateViewCommand) logicalPlan;
			visitor.visit(createView);
		}
	}
}