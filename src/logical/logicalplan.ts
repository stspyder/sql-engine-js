import {LogicalExpression, BooleanBinaryExpression, AggregateExpression} from './logicalexpressions';
import {Schema} from "apache-arrow";
import {DataSource} from "../datasource";

export interface LogicalPlan {
    getSchema(): Promise<Schema>
    getChildren(): Array<LogicalPlan>
    toString(): string
}

export class Scan implements LogicalPlan {
    private readonly path: string;
    private schema: Schema;

    readonly dataSource: DataSource;
    readonly projection: Array<string>;


    constructor(path: string, dataSource: DataSource, projection: Array<string> = []) {
        this.path = path;
        this.dataSource = dataSource;
        this.projection = projection;
    }

    // getDataSource(): DataSource {
    //     return this.dataSource;
    // }
    //
    // getProjection(): Array<string> {
    //     return this.projection;
    // }

    getChildren(): Array<LogicalPlan> {
        return new Array<LogicalPlan>();
    }

    async getSchema(): Promise<Schema> {
        if (this.schema == undefined) {
            this.schema = await this.dataSource.schema();
        }
        if (this.projection.length == 0) {
            return this.schema;
        } else {
            return this.schema.select(...this.projection);
        }
    }

    toString(): string {
        if (this.projection.length == 0) {
            return `Scan: ${this.path}; projection=None`
        } else {
            return `Scan: ${this.path}; projection=${this.projection}`
        }
    }
}

export class Projection implements LogicalPlan {
    readonly input: LogicalPlan;
    readonly expressions: Array<LogicalExpression>;

    constructor(input: LogicalPlan, expressions: Array<LogicalExpression>) {
        this.input = input;
        this.expressions = expressions;
    }

    getChildren(): Array<LogicalPlan> {
        return new Array<LogicalPlan>(this.input);
    }

    async getSchema(): Promise<Schema> {
        let projectedFields = await Promise.all(this.expressions.map(expr => expr.toField(this.input)));
        return new Schema(projectedFields);
    }

    toString(): string {
        let exprString = this.expressions.map(expr => expr.toString()).join(", ");
        return `Projection: ${exprString}`;
    }
}

export class Selection implements LogicalPlan {
    readonly input: LogicalPlan;
    readonly booleanExpression: BooleanBinaryExpression;

    constructor(input: LogicalPlan, booleanExpression: BooleanBinaryExpression) {
        this.input = input;
        this.booleanExpression = booleanExpression;
    }

    getChildren(): Array<LogicalPlan> {
        return new Array<LogicalPlan>(this.input);
    }

    async getSchema(): Promise<Schema> {
        return await this.input.getSchema();
    }

    toString(): string {
        return `Selection ${this.booleanExpression}`
    }
}

export class Aggregate implements LogicalPlan {
    private readonly input: LogicalPlan;
    private readonly groupingExpression: Array<LogicalExpression>;
    private readonly aggregateExpression: Array<AggregateExpression>;

    constructor(input: LogicalPlan, groupingExpression: Array<LogicalExpression>,
      aggregateExpression: Array<AggregateExpression>) {
        this.input = input;
        this.groupingExpression = groupingExpression;
        this.aggregateExpression = aggregateExpression;
    }

    getChildren(): Array<LogicalPlan> {
        return new Array<LogicalPlan>(this.input);
    }

    async getSchema(): Promise<Schema> {
        let groupFields = await Promise.all(this.groupingExpression.map(expr => expr.toField(this.input)));
        let aggFields = await Promise.all(this.aggregateExpression.map(expr => expr.toField(this.input)));
        return new Schema(groupFields.concat(aggFields));
    }
}


export function format(plan: LogicalPlan, indent: number = 0): string {
    let planStr = "";
    for (let i = 0; i < indent; i++) {
        planStr += "\t";
    }

    planStr += plan.toString() + "\n";

    plan.getChildren().forEach(child => {
        planStr += format(child, indent + 1)
    })

    return planStr
}
