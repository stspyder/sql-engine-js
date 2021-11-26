import {CSVDataSource} from "../src/datasource";

describe("A suite is just a function", () => {
    const file = new File(["col1,col2\nSriram,1\nHarini,2\n"], "testFile.csv", {type: "text/csv"});
    const dataSource = new CSVDataSource(file)

    it("and so is a spec", () => {
        dataSource.scan(["col1", "col2"])
    });

});


