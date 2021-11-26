import {LogicalPlan, Projection, Scan, Selection} from "../logical/logicalplan";
import {PhysicalPlan, ProjectionExec, ScanExec, SelectionExec} from "../physical/physicalplan";
import {
    And,
    BinaryExpression,
    Column,
    Equals,
    LiteralInt,
    LiteralString,
    LogicalExpression, NotEquals, Or
} from "../logical/logicalexpressions";
import {
    AndExpression,
    ColumnExpression, EqExpression,
    LiteralNumberExpression,
    LiteralStringExpression, OrExpression,
    PhysicalExpression
} from "../physical/physicalexpressions";
import {Schema} from "apache-arrow";
import {IllegalStateError, SQLError} from "../errors";

export class QueryPlanner {

    async createPhysicalPlan(lp: LogicalPlan): Promise<PhysicalPlan> {
        if (lp instanceof Scan) {
            return new ScanExec(lp.dataSource, lp.projection);
        } else if (lp instanceof Projection) {
            let input = await this.createPhysicalPlan(lp.input);
            let projectionExpressions = await Promise.all(lp.expressions.map(e => this.createPhysicalExpr(e, lp.input)));
            let projectedFields = await Promise.all(lp.expressions.map(e => e.toField(lp.input)));
            let projectionSchema = new Schema(projectedFields);
            return new ProjectionExec(input, projectionSchema, projectionExpressions);
        } else if (lp instanceof Selection) {
            let input = await this.createPhysicalPlan(lp.input);
            let selectionExpression = await this.createPhysicalExpr(lp.booleanExpression, lp.input);
            return new SelectionExec(input, selectionExpression);
        }
    }


    async createPhysicalExpr(le: LogicalExpression, lp: LogicalPlan): Promise<PhysicalExpression> {
        if (le instanceof LiteralString) {
            return new LiteralStringExpression(le.value);
        } else if (le instanceof LiteralInt) {
            return new LiteralNumberExpression(le.value);
        } else if (le instanceof Column) {
            let schema = await lp.getSchema();
            let index = schema.fields.map(f => f.name).indexOf(le.name);
            if (index != -1) {
                return new ColumnExpression(index);
            } else {
                throw new SQLError("No column named: " + le.name);
            }
        } else if (le instanceof BinaryExpression) {
            let left = await this.createPhysicalExpr(le.left, lp);
            let right = await this.createPhysicalExpr(le.right, lp)

            // Check the subtype
            // Comparison Expressions
            if (le instanceof Equals) {
                return new EqExpression(left, right);
            } else if (le instanceof And) {
                return new AndExpression(left, right);
            } else if (le instanceof Or) {
                return new OrExpression(left, right);
            // TODO NotEquals, LessThan, GreaterThan
            } else {
                throw new IllegalStateError("Unsupported binary expression: " + le.toString());
            }
            // TODO Math Expressions
        } else {
            throw new IllegalStateError("Unsupported logical expression: " + le.toString());
        }
    }
}