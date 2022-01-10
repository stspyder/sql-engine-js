import {CSVParser} from "./parsers/csv";
import {NumberVector, RecordBatch, StringVector, Vector} from "./vectors";
import {Schema} from "./schema";
import {DataType} from "./types";


export interface DataSource {
    schema(): Promise<Schema>;
    scan(projection: Array<string>): AsyncGenerator<RecordBatch, void, void>;
}

export class CSVDataSource implements DataSource {
    private readonly file: File;
    private readonly _schema?: Schema;
    private readonly parser: CSVParser;
    private readonly firstLineHeader: boolean;

    constructor(file: File, schema: Schema, firstLineHeader: boolean = true) {
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
            let allColumnVectors = new Array<Vector<any>>();
            let actualProjection = projection.length == 0? schema.fields.map(f => f.name) : projection;
            for (let projectedField of actualProjection) {
                // TODO get rows data by name and not by index
                for (let [index, field] of schema.fields.entries()) {
                    if (projectedField != field.name) {
                        continue;
                    }

                    switch (field.type) {
                        case DataType.String: {
                            let data = rows.map(row => row[index] as string);
                            let stringVector = new StringVector(data);
                            allColumnVectors.push(stringVector);
                            break;
                        }

                        case DataType.Number: {
                            let data = rows.map(row => row[index] as number);
                            let numberVector = new NumberVector(data);
                            allColumnVectors.push(numberVector);
                            break;
                        }

                        default: {
                            console.log(field.type)
                            let data = rows.map(row => row[index]);
                            let stringVector = new StringVector(data);
                            allColumnVectors.push(stringVector);
                            break;
                        }
                    }
                }
            }
            yield new RecordBatch(schema, allColumnVectors);
        }
    }

    async schema(): Promise<Schema> {
        // if (this._schema) {
            // return new Promise<Schema>((resolve, reject) => {
            //     resolve(this._schema);
            // });
        return this._schema;
        // }
        // TODO parser does not support schema detection
    }

}
