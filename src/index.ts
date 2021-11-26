import {CSVDataSource} from "./datasource";
import {Field, Schema, Utf8} from "apache-arrow";
import {format, Projection, Scan, Selection} from "./logical/logicalplan";
import {Column, Equals, LiteralString} from "./logical/logicalexpressions";
import {QueryPlanner} from "./planner/queryplanner";

import * as sqlParser from "js-sql-parser";



export async function start(file: File) {
    let fields = [
        new Field<Utf8>("day", new Utf8()),
        new Field<Utf8>("dsp_name", new Utf8()),
        new Field<Utf8>("buyer_name", new Utf8()),
        new Field<Utf8>("ad_spend", new Utf8()),
    ];

    const csvDataSource = new CSVDataSource(file, new Schema<any>(fields));
    const scan = new Scan(file.name, csvDataSource);
    const selection = new Selection(scan, new Equals(new Column("buyer_name"), new LiteralString("Automatad Inc.")));
    const projection = new Projection(selection, [ new Column("ad_spend"), new Column("day"), new Column("dsp_name"), new Column("buyer_name")])

    console.log(format(projection));

    const queryPlanner = new QueryPlanner();
    const physicalPlan = await queryPlanner.createPhysicalPlan(projection);

    for await (const recordBatch of physicalPlan.execute()) {
        for (let r=0; r<recordBatch.rowCount(); r++) {
            let row = []
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
