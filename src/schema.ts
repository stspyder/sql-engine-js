import {DataType} from "./types";

export class Field {
    constructor(readonly name: string, readonly type: DataType) {}
}

export class Schema {
    private readonly fieldsMap: Map<string, Field>;
    constructor(readonly fields: Array<Field>) {
        this.fieldsMap = new Map<string, Field>(
            fields.map(f => [f.name, f])
        );
    }

    select(...fieldNames: Array<string>): Schema {
        let selectedFields = fieldNames.map(fn => {
            let field = this.fieldsMap.get(fn);
            if (field === undefined) {
                throw new Error(`Unknown field name: ${fn}`);
            }
            return field;
        });

        return new Schema(selectedFields);
    }
}
