import {LogicalPlan, Scan} from "./logicalplan";
import {DataSource} from "../datasource";
import {Data} from "apache-arrow";

export interface LogicalPlanner {
    buildPlan(sqlAST: any): LogicalPlan
}

class Planner implements LogicalPlanner {
    private registeredTables: Map<string, DataSource>

    constructor(registeredTables: Map<string, DataSource>) {
        this.registeredTables = registeredTables;
    }


    planSource(ast: any, plan: LogicalPlan = null): LogicalPlan {
        switch (ast.type) {
            case "TableReferences":
                if (ast.value.length > 1) {
                    throw new Error("Selecting from multiple tables not supported");
                }
                for (const val of ast.value) {
                    return this.planSource(val);
                }
                break;
            case "TableReference":
                return this.planSource(ast.value);
            case "TableFactor":



        }
    }


    planForSources(sqlAST: any): LogicalPlan | null {
        let queryType = sqlAST.value.type;
        if (queryType != "Select") {
            throw new Error("SQL is not a valid SELECT query")
        }

        // First, determine the source
        let from = sqlAST.value.from;
        if (from.type != "TableReferences") {
            throw new Error("Unknown FROM type: " + from.type);
        }

        if (from.value.length > 1) {
            throw new Error("Selecting from multiple tables are not supported")
        }

        for (const fromValue of from.value) {
            if (fromValue.type != "TableReference") {
                throw new Error("Unknown type for FROM value: " + fromValue.type);
            }

            let tableRefValue = fromValue.value;
            if (tableRefValue.type == "SubQuery") {
                throw new Error("Sub-Queries are not supported yet!");
            }

            if (tableRefValue.type == "LeftRightJoinTable" || tableRefValue.type == "InnerCrossJoinTable") {
                throw new Error("Joins are not supported")
            }

            let tableFactorValue = tableRefValue.value;
            if (tableFactorValue.type == "Identifier") {
                // This refers to a table name
                let tableName = tableFactorValue.value;
                return new Scan(tableName, this.registeredTables.get(tableName));
            }
        }
    }

    planExpression(sqlAST: any): LogicalPlan {

    }

    buildPlan(sqlAST: any): LogicalPlan {


        // Lets do the select part
        let selectItems = sqlAST.value.selectItems
        if (selectItems.type != "SelectExpr") {
            throw new Error("Unknown select items type")
        }

        for (const item of selectItems.value) {
            let expr = null;
            switch (item.type) {
                case "Identifier":
                    // This is a column name
                    expr =

            }
        }
    }
}