export class CSVParser {
    private readonly file: File;
    private readonly chunkSize: number;

    constructor(file: File, chunkSizeMB: number = 2) {
        this.file = file;
        this.chunkSize = chunkSizeMB * 1024 * 1024;
    }

    async* readChunk(): AsyncGenerator<string, void, void> {
        for (let byteIndex = 0; byteIndex < this.file.size; byteIndex += this.chunkSize) {
            let startByte = byteIndex;
            let endByte = startByte + this.chunkSize;
            let blob = this.file.slice(startByte, endByte)
            yield await blob.text()
        }
    }

    async preview(): Promise<Array<any>> {
        for await (let chunk of this.readChunk()) {
            let [arr, _] = this.parseString(chunk)
            if (arr.length == 0) {
                throw Error("Error generating preview")
            } else {
                return arr
            }
        }
    }


    parseString(data: string): [Array<any>, number] {
        let arr = [];
        let quote = false;  // 'true' means we're inside a quoted field
        let rowStartIndex = 0;

        // Iterate over each character, keep track of current row and column (of the returned array)
        for (let row = 0, col = 0, c = 0; c < data.length; c++) {
            let cc = data[c], nc = data[c+1];        // Current character, next character
            arr[row] = arr[row] || [];             // Create a new row if necessary
            arr[row][col] = arr[row][col] || '';   // Create a new column (start with empty string) if necessary

            // If the current character is a quotation mark, and we're inside a
            // quoted field, and the next character is also a quotation mark,
            // add a quotation mark to the current column and skip the next character
            if (cc == '"' && quote && nc == '"') {
                arr[row][col] += cc;
                ++c;
                continue;
            }

            // If it's just one quotation mark, begin/end quoted field
            if (cc == '"') {
                quote = !quote;
                continue;
            }

            // If it's a comma and we're not in a quoted field, move on to the next column
            if (cc == ',' && !quote) {
                ++col;
                continue;
            }

            // If it's a newline (CRLF) and we're not in a quoted field, skip the next character
            // and move on to the next row and move to column 0 of that new row
            if (cc == '\r' && nc == '\n' && !quote) {
                ++row;
                col = 0;
                ++c;
                continue;
            }

            // If it's a newline (LF or CR) and we're not in a quoted field,
            // move on to the next row and move to column 0 of that new row
            if (cc == '\n' && !quote) {
                ++row;
                rowStartIndex = c + 1;
                col = 0;
                continue;
            }
            if (cc == '\r' && !quote) {
                ++row;
                rowStartIndex = c + 1;
                col = 0;
                continue;
            }

            // Otherwise, append the current character to the current column
            arr[row][col] += cc;
        }
        return [arr, rowStartIndex]
    }

    async* parseRows() {
        let leftOverFromPreviousChunk = '';
        let columns = (await this.preview())[0]
        for await (let chunk of this.readChunk()) {
            if (leftOverFromPreviousChunk.length != 0) {
                chunk = leftOverFromPreviousChunk + chunk
                leftOverFromPreviousChunk = ''
            }

            let [arr, rowStartIndex] = this.parseString(chunk)

            if (arr[arr.length - 1].length < columns.length) {
                // Incomplete parsing of last row
                arr = arr.slice(0, arr.length - 1)
                leftOverFromPreviousChunk = chunk.slice(rowStartIndex)
            }
            if (arr.length != 0) {
                yield arr;
            }
        }

        // All chunks have been consumed. See if something is still leftover
        if (leftOverFromPreviousChunk.length != 0) {
            let [arr, _] = this.parseString(leftOverFromPreviousChunk);
            yield arr;
        }
    }
}