import {CSVDataSource, DataSource} from "../src/datasource";
import {QueryPlanner} from "../src/planner/queryplanner";
import * as sqlParser from "js-sql-parser";
import {Planner} from "../src/logical/planner";
import {Field, Schema} from "../src/schema";
import {DataType} from "../src/types";


async function execute(dataSource: DataSource, tableName: string, sqlQuery: string): Promise<[Schema, Array<Array<any>>]> {
    const logicalPlanner = new Planner(new Map<string, DataSource>([
        [tableName, dataSource],
    ]));

    const ast = sqlParser.parse(sqlQuery);
    const logicalPlan = logicalPlanner.buildPlan(ast);
    const physicalPlanner = new QueryPlanner();
    const physicalPlan = await physicalPlanner.createPhysicalPlan(logicalPlan);

    let schema = await physicalPlan.getSchema();
    let rows = [];
    for await (const recordBatch of physicalPlan.execute()) {
        for (let r=0; r<recordBatch.rowCount(); r++) {
            let row = [];
            for (let c = 0; c < recordBatch.columnCount(); c++) {
                row.push(recordBatch.get(c).get(r))
            }
            rows.push(row);
        }
    }

    return [schema, rows];
}

type ExpectedField = [name: string, type: DataType];

function assertSchema(actualSchema: Schema, expectedFields: Array<ExpectedField>) {
    actualSchema.fields.forEach((f, i) => {
        expect(f.name).toBe(expectedFields[i][0]);
        expect(f.type).toBe(expectedFields[i][1]);
    });
}


describe("When SQL Engine executes", () => {
  const csvContents =
  `"Month", "1958", "1959", "1960"
  "JAN",  340,  360,  417
  "FEB",  318,  342,  391
  "MAR",  362,  406,  419
  "APR",  348,  396,  461
  "MAY",  363,  420,  472
  "JUN",  435,  472,  535
  "JUL",  491,  548,  622
  "AUG",  505,  559,  606
  "SEP",  404,  463,  508
  "OCT",  359,  407,  461
  "NOV",  310,  362,  390
  "DEC",  337,  405,  432`;

  const simpleFile = new File([csvContents], "airtravel.csv", {type: "text/csv"});

  let fields = [
      new Field("Month", DataType.String),
      new Field("1958", DataType.Number),
      new Field("1959", DataType.Number),
      new Field("1960", DataType.Number),
  ];

  const csvDataSource = new CSVDataSource(simpleFile, new Schema(fields));
  const tableName = "air_travel";

  describe("a non aggregate sql it", () => {

    it("supports projection", async () => {
      const sql = "SELECT Month, `1958`, `1959` FROM air_travel";
      const [actualSchema, actualResults] = await execute(csvDataSource, tableName, sql);
      assertSchema(actualSchema, [
          ["Month", DataType.String],
          ["1958", DataType.Number],
          ["1959", DataType.Number]
      ]);

      const expectedResults = [
          ["JAN",  340,  360],
          ["FEB",  318,  342],
          ["MAR",  362,  406],
          ["APR",  348,  396],
          ["MAY",  363,  420],
          ["JUN",  435,  472],
          ["JUL",  491,  548],
          ["AUG",  505,  559],
          ["SEP",  404,  463],
          ["OCT",  359,  407],
          ["NOV",  310,  362],
          ["DEC",  337,  405]
      ];

      expect(actualResults.length).toBe(expectedResults.length);
    });


  });

});
