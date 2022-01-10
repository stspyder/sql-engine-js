export const enum DataType {
    NULL,
    Boolean,
    String,
    Number,
    BigInt,
    Decimal,
    Date,
    DateTime,
    // TODO Add more types
}


export const enum BooleanOperator {
    Equals = "=",
    NotEquals = "!=",
    GraterThan = ">",
    LessThan = "<",
    GreaterThanEquals = ">=",
    LessThanEquals = "<=",
    And = "AND",
    Or = "OR",
}

export const enum MathOperator {
    Add = "+",
    Subtract = "-",
    Multiply = "*",
    Divide = "/",
}
