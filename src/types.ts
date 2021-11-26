import {Schema} from "apache-arrow";
import {Vector} from "apache-arrow/vector";

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
