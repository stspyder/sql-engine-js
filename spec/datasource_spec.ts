import {CSVDataSource} from "../src/datasource";

describe("When a CSV data source is created and is scanned", () => {
    const file = new File(["col1,col2\nSriram,1\nHarini,2\n"], "testFile.csv", {type: "text/csv"});
    const dataSource = new CSVDataSource(file)

    it("it should return a record batch with right data", async () => {
        let allRows = [];
        for await (const recordBatch of dataSource.scan(["col1", "col2"])) {
          expect(recordBatch.rowCount()).toBe(2);
          for (let r=0; r<recordBatch.rowCount(); r++) {
            let row = [];
            expect(recordBatch.columnCount()).toBe(2);
            for (let c = 0; c < recordBatch.columnCount(); c++) {
                row.push(recordBatch.get(c).get(r))
            }
            allRows.push(row);
          }
        }

        expect(allRows).toHaveSize(2);
        expect(allRows[0]).toHaveSize(2);
        expect(allRows[0][0]).toBe("Sriram");
        expect(allRows[0][1]).toBe("1");
        expect(allRows[1]).toHaveSize(2);
        expect(allRows[1][0]).toBe("Harini");
        expect(allRows[1][1]).toBe("2");
    });

    it("it should return a record batch with right order of data", async () => {
        let allRows = [];
        for await (const recordBatch of dataSource.scan(["col2", "col1"])) {
          expect(recordBatch.rowCount()).toBe(2);
          for (let r=0; r<recordBatch.rowCount(); r++) {
            let row = [];
            expect(recordBatch.columnCount()).toBe(2);
            for (let c = 0; c < recordBatch.columnCount(); c++) {
                row.push(recordBatch.get(c).get(r))
            }
            allRows.push(row);
          }
        }

        expect(allRows).toHaveSize(2);
        expect(allRows[0]).toHaveSize(2);
        expect(allRows[0][0]).toBe("1");
        expect(allRows[0][1]).toBe("Sriram");
        expect(allRows[1]).toHaveSize(2);
        expect(allRows[1][0]).toBe("2");
        expect(allRows[1][1]).toBe("Harini");
    });

    it("it should return a record batch with projected fields", async () => {
        let allRows = [];
        for await (const recordBatch of dataSource.scan(["col2"])) {
          expect(recordBatch.rowCount()).toBe(2);
          for (let r=0; r<recordBatch.rowCount(); r++) {
            let row = [];
            expect(recordBatch.columnCount()).toBe(1);
            for (let c = 0; c < recordBatch.columnCount(); c++) {
                row.push(recordBatch.get(c).get(r))
            }
            allRows.push(row);
          }
        }

        expect(allRows).toHaveSize(2);
        expect(allRows[0]).toHaveSize(1);
        expect(allRows[0][0]).toBe("1");
        expect(allRows[1]).toHaveSize(1);
        expect(allRows[1][0]).toBe("2");
    });
});
