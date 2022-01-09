import {
    Bool,
    DataType,
    Field, Int16Vector,
    Int32,
    Int32Vector,
    Schema,
    Type,
    Utf8,
    Utf8Vector
} from 'apache-arrow'

import {CSVParser} from "./parsers/csv";
import {Vector} from "apache-arrow/vector";
import {RecordBatch} from "./vectors";


export interface DataSource {
    schema(): Promise<Schema>;
    scan(projection: Array<string>): AsyncGenerator<RecordBatch, void, void>;
}

export class CSVDataSource implements DataSource {
    private readonly file: File;
    private _schema?: Schema;
    private readonly parser: CSVParser;
    private readonly firstLineHeader: boolean;

    constructor(file: File, schema?: Schema, firstLineHeader: boolean = true) {
        this.file = file;
        this._schema = schema;
        this.firstLineHeader = firstLineHeader;
        this.parser = new CSVParser(this.file);
    }

    async* scan(projection: Array<string>): AsyncGenerator<RecordBatch, void, void> {
        let schema = await this.schema();
        let headerRemoved = false;
        for await (let rows of this.parser.parseRows()) {
            console.log("CSV Data Source: Parsed batch with size: " + rows.length);

            if (!headerRemoved && this.firstLineHeader) {
                rows = rows.slice(1);
            }
            let allColumnVectors = new Array<Vector>();
            let actualProjection = projection.length == 0? schema.fields.map(f => f.name) : projection;
            for (let projectedField of actualProjection) {
                // TODO get rows data by name and not by index
                for (let [index, field] of schema.fields.entries()) {
                    if (projectedField != field.name) {
                        continue;
                    }

                    switch (field.typeId) {
                        case Type.Utf8: {
                            let mappedRows = rows.map(row => row[index] as string);
                            let utf8Vector = Utf8Vector.from(mappedRows);
                            allColumnVectors.push(utf8Vector);
                            break;
                        }

                        case Type.Int: {
                            let int32Array = Int32Array.of(...rows.map(row => row[index] as number));
                            let int32Vector = Int32Vector.from(int32Array);
                            allColumnVectors.push(int32Vector);
                            break;
                        }

                        case Type.Int16: {
                            let int16Array = Int16Array.of(...rows.map(row => row[index] as number));
                            let int16Vector = Int16Vector.from(int16Array);
                            allColumnVectors.push(int16Vector);
                            break;
                        }

                        default: {
                            console.log(field.typeId)
                            let utf8Vector = Utf8Vector.from(rows.map(row => row[index]));
                            allColumnVectors.push(utf8Vector);
                            break;
                        }
                    }
                }
            }
            yield new RecordBatch(schema, allColumnVectors);
        }
    }

    async schema(): Promise<Schema> {
        if (this._schema) {
            // return new Promise<Schema>((resolve, reject) => {
            //     resolve(this._schema);
            // });
            return this._schema;
        }

        // TODO parser does not support schema detection
        let preview = await this.parser.preview()
        let fields = preview[0].map((fieldName, i) => {
            let rowValue = preview[0][i];
            switch (typeof rowValue) {
                case "string": {
                    // TODO check for bigint and dates
                    return new Field(fieldName, new Utf8());
                }
                case "number": {
                    return new Field(fieldName, new Int32());
                }
                case "boolean": {
                    return new Field(fieldName, new Bool());
                }
                default: {
                    return new Field(fieldName, new Utf8());
                }
            }
        });
        this._schema = new Schema(fields);
        return this._schema;
    }

}
