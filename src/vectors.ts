import {DataType} from "./types";
import {Schema} from "./schema";

export interface Vector<T> {
    get(i: number): T;
    getSize(): number;
    getType(): DataType;
}

export abstract class TypedVector<T> implements Vector<T> {
    private readonly data: Array<T>;

    constructor(data: Array<T>) {
        this.data = data;
    }

    get(i: number): T {
        return this.data[i];
    }

    getSize(): number {
        return this.data.length;
    }

    abstract getType(): DataType
}

export class StringVector extends TypedVector<string> {
    getType(): DataType {
        return DataType.String;
    }
}

export class NumberVector extends TypedVector<number> {
    getType(): DataType {
        return DataType.Number;
    }
}

export class BooleanVector extends TypedVector<boolean> {
    getType(): DataType {
        return DataType.Boolean;
    }

}

export abstract class LiteralValueVector<T> implements Vector<T> {

    private readonly value: T;
    private readonly size: number;

    constructor(value: T, size: number) {
        this.value = value;
        this.size = size
    }

    get(i: number): T {
        return this.value
    }

    getSize(): number {
        return this.size;
    }

    abstract getType(): DataType;
}

export class LiteralStringVector extends LiteralValueVector<string> {
    getType(): DataType {
        return DataType.String;
    }
}

export class LiteralNumberVector extends LiteralValueVector<number> {
    getType(): DataType {
        return DataType.Number;
    }
}

export class RecordBatch {

    private readonly schema: Schema
    private readonly vectors: Array<Vector<any>>;

    constructor(schema: Schema, vectors: Array<Vector<any>>) {
        this.schema = schema
        this.vectors = vectors
    }

    rowCount(): number {
        return this.vectors[0].getSize();
    }

    columnCount(): number {
        return this.vectors.length;
    }

    get(i: number): Vector<any> {
        return this.vectors[i];
    }

    getSchema(): Schema {
        return this.schema;
    }
}
