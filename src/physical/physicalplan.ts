import {BoolVector, DataType, Int32Vector, Schema, Type, Utf8Vector} from "apache-arrow";
import {DataSource} from "../datasource";
import {PhysicalExpression} from "./physicalexpressions";
import {RecordBatch} from "../vectors";
import {IllegalStateError} from "../errors";

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
            let resultVector = this.expression.evaluate(recordBatch) as BoolVector;
            if (resultVector.length != recordBatch.rowCount()) {
                throw new IllegalStateError("row count mismatch when evaluating expression")
            }
            let schema = recordBatch.getSchema();
            let filteredVectors = schema.fields.map((field, index) => {
                let fieldType = field.type as DataType;
                let columnVector = recordBatch.get(index);

                let rows = [];
                for (let r = 0; r < columnVector.length; r++) {
                    if (resultVector.get(r)) {
                        let value = columnVector.get(r);
                        rows.push(value);
                    }
                }

                // TODO this cannot scale as we start supporting more types
                switch (fieldType.typeId) {
                    case Type.Utf8: {
                        return Utf8Vector.from(rows);
                    }

                    case Type.Int: {
                        let int32Array = Int32Array.of(...rows);
                        return Int32Vector.from(int32Array);
                    }

                    default: {
                        return Utf8Vector.from(rows);
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