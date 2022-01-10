import {LogicalPlan, Projection, Scan, Selection} from "./logicalplan";
import {DataSource} from "../datasource";
import {
    Add, Alias, And,
    BooleanBinaryExpression,
    Column,
    Divide, Equals, GreaterThan, GreaterThanEquals, IsNull, LessThan, LessThanEquals, LiteralNumber, LiteralString,
    LogicalExpression,
    Multiply, NotEquals, Or,
    Subtract,
    Sum
} from "./logicalexpressions";
import {SQLError} from "../errors";

export interface LogicalPlanner {
    buildPlan(sqlAST: any): LogicalPlan
}

export class Planner implements LogicalPlanner {
    private registeredTables: Map<string, DataSource>;

    constructor(registeredTables: Map<string, DataSource>) {
        this.registeredTables = registeredTables;
    }


    planSource(ast: any): LogicalPlan {
        switch (ast.type) {
            case "TableReferences":
                let tableReferences = ast.value;
                if (tableReferences.length > 1) {
                    throw new Error("Selecting from multiple tables not supported");
                }
                return this.planTableReference(tableReferences[0]);
            default:
                throw new Error(`Unknown FROM type ${ast.type}`);
        }
    }


    planTableReference(tableReferenceAst: any): LogicalPlan {
        let tableReference = tableReferenceAst.value;
        switch(tableReference.type) {
            case "LeftRightJoinTable":
                return this.planLeftRightJoin(tableReference);
            case "InnerCrossJoinTable":
                return this.planInnerJoin(tableReference);
            case "TableFactor":
                return this.planTableFactor(tableReference);
            default:
                throw new Error(`Unknown table reference type: ${tableReference.type}`);
        }
    }


    planTableFactor(tableFactorAst: any): LogicalPlan {
        // TODO process alias information here
        let tableFactor = tableFactorAst.value;
        switch (tableFactor.type) {
            case "Identifier":
                let tableName = tableFactor.value;
                if (this.registeredTables.has(tableName)) {
                    return new Scan(tableName, this.registeredTables.get(tableName));
                } else {
                    throw new Error(`Cannot find table '${tableName}' in the list of registered tables.`);
                }
            default:
                throw new Error(`Unknown TableFactor type: '${tableFactor.type}`);
        }
    }


    planLeftRightJoin(leftRightJoinAst: any): LogicalPlan {
        switch (leftRightJoinAst.leftRight) {
            default:
                throw new Error("Left or Right Joins are not supported yet")
        }
    }


    planInnerJoin(innerJoinAst: any): LogicalPlan {
        throw new Error("Inner joins are not supported yet")
    }

    planSelectItems(selectItemsAst: any, inputPlan: LogicalPlan): LogicalPlan {
        switch (selectItemsAst.type) {
            case "SelectExpr":
                let selectExpressions = selectItemsAst.value;
                let logicalExpressions = selectExpressions.map(exp => this.planExpression(exp));
                return new Projection(inputPlan, logicalExpressions);
            default:
                throw new Error(`Unknown Select items type: '${selectItemsAst.type}'`)
        }
    }

    wrapWithAlias(alias: string, input: LogicalExpression): LogicalExpression {
        if (alias == null) {
            return input;
        } else if (alias.trim().length == 0) {
            throw new SQLError(`Alias for expression ${input.toString()} is not valid`);
        } else {
            return new Alias(alias, input);
        }
    }

    planExpression(expr: any): LogicalExpression {
        switch (expr.type) {
            case "Number":
                return this.wrapWithAlias(expr.alias, new LiteralNumber(Number(expr.value)));
            case "String":
                // Remove quotes
                let value = expr.value.substring(1, expr.value.length - 1);
                return this.wrapWithAlias(expr.alias, new LiteralString(value));
            case "Identifier":
                // Remove back ticks if present
                if (expr.value.startsWith("`")) {
                    return this.wrapWithAlias(expr.alias, new Column(expr.value.substring(1, expr.value.length - 1)));
                }

                return this.wrapWithAlias(expr.alias, new Column(expr.value));
            case "FunctionCall":
                return this.wrapWithAlias(expr.alias, this.planFunction(expr));
            case "BitExpression":
                return this.wrapWithAlias(expr.alias, this.planBitExpression(expr));
            case "ComparisonBooleanPrimary":
                return this.planBooleanExpression(expr);
            case "AndExpression":
                return new And(this.planExpression(expr.left), this.planExpression(expr.right));
            case "OrExpression":
                return new Or(this.planExpression(expr.left), this.planExpression(expr.right));
            case "IsNullBooleanPrimary":
                return new IsNull(this.planExpression(expr.value));
            default:
                throw new Error(`Unknown Expression Type '${expr.type}'`);
        }
    }

    planBitExpression(bitExpression: any): LogicalExpression {
        switch (bitExpression.operator) {
            case "*":
                return new Multiply(this.planExpression(bitExpression.left), this.planExpression(bitExpression.right));
            case "+":
                return new Add(this.planExpression(bitExpression.left), this.planExpression(bitExpression.right));
            case "-":
                return new Subtract(this.planExpression(bitExpression.left), this.planExpression(bitExpression.right));
            case "/":
                return new Divide(this.planExpression(bitExpression.left), this.planExpression(bitExpression.right));
            default:
                throw new Error(`Unknown operator ${bitExpression.operator} in SQL statement`)
        }
    }

    planBooleanExpression(booleanAst: any): LogicalExpression {
        switch(booleanAst.operator) {
            case "=":
                return new Equals(this.planExpression(booleanAst.left), this.planExpression(booleanAst.right));
            case "<>":
                return new NotEquals(this.planExpression(booleanAst.left), this.planExpression(booleanAst.right));
            case ">":
                return new GreaterThan(this.planExpression(booleanAst.left), this.planExpression(booleanAst.right));
            case ">=":
                return new GreaterThanEquals(this.planExpression(booleanAst.left), this.planExpression(booleanAst.right));
            case "<":
                return new LessThan(this.planExpression(booleanAst.left), this.planExpression(booleanAst.right));
            case "<=":
                return new LessThanEquals(this.planExpression(booleanAst.left), this.planExpression(booleanAst.right));

        }
    }

    planFunction(functionCall: any): LogicalExpression {
        switch (functionCall.name.toUpperCase()) {
            case "SUM":
                if (functionCall.params.length > 1) {
                    throw new Error("Aggregate function SUM takes one parameter. More than one provided.")
                }
                return new Sum(this.planExpression(functionCall.params[0]));
            default:
                throw new Error(`Function ${functionCall.name.toUpperCase()} is not supported.`)
        }
    }

    planSelection(whereAst: any, sourcePlan: LogicalPlan): LogicalPlan {
        let selectExpression = this.planExpression(whereAst);

        if (!(selectExpression instanceof BooleanBinaryExpression)) {
            throw new Error("WHERE clause in the SQL statement did not evaluate to a boolean expression")
        }

        if (sourcePlan != null) {
            return new Selection(sourcePlan, selectExpression)
        } else {
            return null;
        }
    }

    buildPlan(sqlAST: any): LogicalPlan {
        let main = sqlAST.value;
        switch (main.type) {
            case "Select":
                let sourcePlan = main.from == null? null : this.planSource(main.from);
                let predicatePlan = main.where == null? null : this.planSelection(main.where, sourcePlan);
                if (predicatePlan != null) {
                    return this.planSelectItems(main.selectItems, predicatePlan);
                } else {
                    return this.planSelectItems(main.selectItems, sourcePlan);
                }
            default:
                throw new Error("SQL query is not a SELECT statement")
        }
    }
}
