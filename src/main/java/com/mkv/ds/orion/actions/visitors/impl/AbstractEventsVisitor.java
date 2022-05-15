package com.mkv.ds.orion.actions.visitors.impl;

import java.util.List;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

import com.mkv.ds.orion.actions.visitors.DbEventsVisitor;
import com.mkv.ds.orion.model.Table;
import com.mkv.ds.orion.model.View;

import scala.Option;
import scala.collection.Iterator;

public abstract class AbstractEventsVisitor implements DbEventsVisitor {

	protected void getChildrenFromRelations(Iterator<LogicalPlan> iter, List<Table> sourceTables,
			List<View> sourceViews) {

		while (iter.hasNext()) {

			LogicalPlan plan = iter.next();

			if (plan instanceof HiveTableRelation) {

				HiveTableRelation htr = (HiveTableRelation) plan;

				TableIdentifier identifier = htr.tableMeta().identifier();

				sourceTables.add(new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase()));

			} else if (plan instanceof LogicalRelation) {

				LogicalRelation lr = (LogicalRelation) plan;

				Option<CatalogTable> catalogTable = lr.catalogTable();

				if (!catalogTable.isEmpty()) {

					TableIdentifier identifier = catalogTable.get().identifier();

					sourceTables.add(new Table(identifier.database().get().toLowerCase(), identifier.table().toLowerCase()));
				}

			} else if (plan instanceof org.apache.spark.sql.catalyst.plans.logical.View) {

				org.apache.spark.sql.catalyst.plans.logical.View view = (org.apache.spark.sql.catalyst.plans.logical.View) plan;

				if (!view.isTempView()) {

					TableIdentifier identifier = view.desc().identifier();

					sourceViews.add(new View(identifier.database().get().toLowerCase(), identifier.table().toLowerCase()));
				}
			} else {
				getChildrenFromRelations(plan.children().toList().iterator(), sourceTables, sourceViews);
			}
		}
	}
}
