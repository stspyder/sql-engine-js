import {BaseVector, Data, DataType, DateDay, Int32, Schema, Utf8} from "apache-arrow";
import {Vector} from "apache-arrow/vector";

export class LiteralValueVector<T extends DataType, R> extends BaseVector<T> {

    private readonly value: R;
    private readonly size: number;

    constructor(value: R, size: number) {
        let buffer = []
        for (let i = 0; i < size; i++) {
            buffer.push(value)
        }
        let data = null;
        switch (typeof value) {
            case "string":
                data = Data.Utf8(new Utf8(), 0, size, 0, null, null, buffer)
                break;
            case "number":
                data = Data.Int(new Int32(), 0, size, 0, null, buffer)
                break
            case "object":
                if (value instanceof Date) {
                    data = Data.Date(new DateDay(), 0, size, 0, null, buffer)
                }
                break
            default:
                data = Data.Utf8(new Utf8(), 0, size, 0, null, null, buffer)
                break;
        }
        super(data)
        this.value = value;
        this.size = size
    }

    get(i: number): R {
        return this.value
    }
}

export class RecordBatch {

    private readonly schema: Schema
    private readonly vectors: Vector[]

    constructor(schema: Schema, vectors: Vector[]) {
        this.schema = schema
        this.vectors = vectors
    }

    rowCount(): number {
        return this.vectors[0].length
    }

    columnCount(): number {
        return this.vectors.length
    }

    get(i: number): Vector {
        return this.vectors[i]
    }

    getSchema(): Schema {
        return this.schema;
    }
}