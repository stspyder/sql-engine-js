import {CSVDataSource, DataSource} from "./datasource";
import {Field, Schema, Utf8, Int32} from "apache-arrow";
import {format} from "./logical/logicalplan";
import {QueryPlanner} from "./planner/queryplanner";

import * as sqlParser from "js-sql-parser";
import {Planner} from "./logical/planner";



export async function start(file: File, sqlQuery: string) {
    let fields = [
        new Field<Utf8>("Month", new Utf8()),
        new Field<Int32>("1958", new Int32()),
        new Field<Int32>("1959", new Int32()),
        new Field<Int32>("1960", new Int32()),
    ];

    const csvDataSource = new CSVDataSource(file, new Schema<any>(fields));
    const logicalPlanner = new Planner(new Map<string, DataSource>([
        ["air_travel", csvDataSource],
    ]))

    let ast = sqlParser.parse(sqlQuery);
    console.log(JSON.stringify(ast, null, 2));

    const logicalPlan = logicalPlanner.buildPlan(ast);
    console.log(format(logicalPlan));

    const physicalPlanner = new QueryPlanner();
    const physicalPlan = await physicalPlanner.createPhysicalPlan(logicalPlan);

    for await (const recordBatch of physicalPlan.execute()) {
        let schema = await physicalPlan.getSchema();
        let fieldNames = schema.fields.map(f => f.name);
        console.log(fieldNames.join(", "));
        for (let r=0; r<recordBatch.rowCount(); r++) {
            let row = [];
            for (let c = 0; c < recordBatch.columnCount(); c++) {
                row.push(recordBatch.get(c).get(r))
            }

            console.log(row.join(", "))
        }
    }
}


export function parseSql(sqlText) {
    let ast = sqlParser.parse(sqlText);
    console.log(JSON.stringify(ast, null, 2));
}
