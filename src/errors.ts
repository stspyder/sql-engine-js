export class SQLError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "SQLError"
    }
}


export class IllegalStateError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "IllegalStateError"
    }
}