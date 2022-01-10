import {DataSource} from "../datasource";
import {PhysicalExpression} from "./physicalexpressions";
import {BooleanVector, NumberVector, RecordBatch, StringVector} from "../vectors";
import {IllegalStateError} from "../errors";
import {Schema} from "../schema";
import {DataType} from "../types";

export interface PhysicalPlan {
    getSchema(): Promise<Schema>
    execute(): AsyncGenerator<RecordBatch, void, void>
    getChildren(): Array<PhysicalPlan>
}

export class ScanExec implements PhysicalPlan {

    private readonly dataSource: DataSource;
    private readonly projection: Array<string>;

    constructor(dataSource: DataSource, projection: Array<string>) {
        this.dataSource = dataSource;
        this.projection = projection;
    }

    async* execute(): AsyncGenerator<RecordBatch, void, void> {
        for await (const recBatch of this.dataSource.scan(this.projection)) {
            yield recBatch;
        }
    }

    getChildren(): Array<PhysicalPlan> {
        return [];
    }

    getSchema(): Promise<Schema> {
        return this.dataSource.schema();
    }
}


export class ProjectionExec implements PhysicalPlan {
    private readonly inputPlan: PhysicalPlan;
    private readonly schema: Schema;
    private readonly expressions: Array<PhysicalExpression>;

    constructor(inputPlan: PhysicalPlan, schema: Schema, expressions: Array<PhysicalExpression>) {
        this.inputPlan = inputPlan;
        this.schema = schema;
        this.expressions = expressions;
    }

    async* execute(): AsyncGenerator<RecordBatch, void, void> {
       for await (const recordBatch of this.inputPlan.execute()) {
           let vectors = this.expressions.map(expr => expr.evaluate(recordBatch))
           // TODO Schema should change here
           yield new RecordBatch(this.schema, vectors)
       }
    }

    getChildren(): Array<PhysicalPlan> {
        return [this.inputPlan];
    }

    getSchema(): Promise<Schema> {
        return Promise.resolve(this.schema);
    }
}


export class SelectionExec implements PhysicalPlan {
    private readonly input: PhysicalPlan;
    private readonly expression: PhysicalExpression;

    constructor(input: PhysicalPlan, expression: PhysicalExpression) {
        this.input = input;
        this.expression = expression;
    }

    async* execute(): AsyncGenerator<RecordBatch, void, void> {
        for await (const recordBatch of this.input.execute()) {
            let booleanVector = this.expression.evaluate(recordBatch) as BooleanVector;
            if (booleanVector.getSize() != recordBatch.rowCount()) {
                throw new IllegalStateError("row count mismatch when evaluating expression")
            }
            let schema = recordBatch.getSchema();
            let filteredVectors = schema.fields.map((field, index) => {
                let fieldType = field.type;
                let columnVector = recordBatch.get(index);

                let rows = [];
                for (let r = 0; r < columnVector.getSize(); r++) {
                    if (booleanVector.get(r)) {
                        let value = columnVector.get(r);
                        rows.push(value);
                    }
                }

                // TODO this cannot scale as we start supporting more types
                switch (field.type) {
                    case DataType.String: {
                        return new StringVector(rows);
                    }

                    case DataType.Number: {
                        return new NumberVector(rows);
                    }

                    case DataType.Boolean: {
                        return new BooleanVector(rows);
                    }

                    default: {
                        return new StringVector(rows);
                    }
                }
            });

            yield new RecordBatch(schema, filteredVectors);
        }
    }

    getChildren(): Array<PhysicalPlan> {
        return [this.input];
    }

    getSchema(): Promise<Schema> {
        return this.input.getSchema();
    }

}


export class HashAggregate implements PhysicalPlan {

    constructor(input: PhysicalPlan, groupExpressions: Array<PhysicalExpression>, ) {
    }

    execute(): AsyncGenerator<RecordBatch, void, void> {
        return undefined;
    }

    getChildren(): Array<PhysicalPlan> {
        return undefined;
    }

    getSchema(): Promise<Schema> {
        return Promise.resolve(undefined);
    }

}
