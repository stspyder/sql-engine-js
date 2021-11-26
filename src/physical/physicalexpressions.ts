import {
    BoolVector,
    Int32,
    Utf8
} from "apache-arrow";
import {Vector} from "apache-arrow/vector";
import {LiteralValueVector, RecordBatch} from "../vectors";


export interface PhysicalExpression {
    evaluate(input: RecordBatch): Vector

    toString(): string
}

export class ColumnExpression implements PhysicalExpression {
    private readonly index: number;

    constructor(index: number) {
        this.index = index;
    }

    evaluate(input: RecordBatch): Vector {
        return input.get(this.index);
    }

    toString(): string {
        return "#" + this.index;
    }
}


export class LiteralNumberExpression implements PhysicalExpression {

    private readonly value: number;

    constructor(value: number) {
        this.value = value;
    }

    evaluate(input: RecordBatch): Vector {
        return new LiteralValueVector<Int32, number>(this.value, input.rowCount());
    }

    toString(): string {
        return this.value.toString();
    }

}

export class LiteralStringExpression implements PhysicalExpression {
    private readonly value: string;

    constructor(value: string) {
        this.value = value;
    }

    evaluate(input: RecordBatch): Vector {
        return new LiteralValueVector<Utf8, string>(this.value, input.rowCount());
    }
}


export abstract class BinaryExpression implements PhysicalExpression {

    private readonly leftExpression: PhysicalExpression;
    private readonly rightExpression: PhysicalExpression;

    constructor(leftExpression, rightExpression: PhysicalExpression) {
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
    }

    evaluate(input: RecordBatch): Vector {
        let leftResult = this.leftExpression.evaluate(input);
        let rightResult = this.rightExpression.evaluate(input);

        if (leftResult.length != rightResult.length) {
            throw Error("Binary expression operands did not produce same number of records")
        }

        if (leftResult.typeId != rightResult.typeId) {
            throw Error("Binary expression operands do not have the same type: "
                + leftResult.type + " <> " + rightResult.type)
        }

        return this.evaluateVectors(leftResult, rightResult);
    }

    abstract evaluateVectors(left: Vector, right: Vector): Vector;
}


// Comparison Expressions
export abstract class BooleanExpression extends BinaryExpression {

    evaluateVectors(left: Vector, right: Vector): Vector {
        const results = new Array<boolean>()
        for (let i = 0; i < left.length; i++) {
            let assertionResult = this.compare(left.get(i), right.get(i))
            results.push(assertionResult)
        }

        return BoolVector.from(results)
    }

    abstract compare(leftValue: any, rightValue: any): boolean
}


export class AndExpression extends BooleanExpression {

    compare(leftValue: any, rightValue: any): boolean {
        return leftValue && rightValue;
    }
}


export class OrExpression extends BooleanExpression {
    compare(leftValue: any, rightValue: any): boolean {
        return leftValue || rightValue;
    }
}

export class EqExpression extends BooleanExpression {
    compare(leftValue: any, rightValue: any): boolean {
        return leftValue == rightValue;
    }
}


export class LtExpression extends BooleanExpression {
    // TODO add type to check less than on other types like dates
    compare(leftValue: any, rightValue: any): boolean {
        return (leftValue as number) < (rightValue as number);
    }
}
