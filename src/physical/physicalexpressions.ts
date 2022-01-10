import {BooleanVector, LiteralNumberVector, LiteralStringVector, NumberVector, RecordBatch, Vector} from "../vectors";
import {DataType} from "../types";
import {SQLError} from "../errors";


export interface PhysicalExpression {
    evaluate(input: RecordBatch): Vector<any>
    toString(): string
}

export class ColumnExpression implements PhysicalExpression {
    private readonly index: number;

    constructor(index: number) {
        this.index = index;
    }

    evaluate(input: RecordBatch): Vector<any> {
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

    evaluate(input: RecordBatch): Vector<Number> {
        return new LiteralNumberVector(this.value, input.rowCount());
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

    evaluate(input: RecordBatch): Vector<String> {
        return new LiteralStringVector(this.value, input.rowCount());
    }
}


export abstract class BinaryExpression implements PhysicalExpression {

    private readonly leftExpression: PhysicalExpression;
    private readonly rightExpression: PhysicalExpression;

    constructor(leftExpression, rightExpression: PhysicalExpression) {
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
    }

    evaluate(input: RecordBatch): Vector<any> {
        let leftResult = this.leftExpression.evaluate(input);
        let rightResult = this.rightExpression.evaluate(input);

        if (leftResult.getSize() != rightResult.getSize()) {
            throw Error("Binary expression operands did not produce same number of records");
        }

        if (leftResult.getType() != rightResult.getType()) {
            throw Error("Binary expression operands do not have the same type: "
                + leftResult.getType() + " <> " + rightResult.getType());
        }

        return this.evaluateVectors(leftResult, rightResult);
    }

    toString(): string {
        return
    }

    abstract evaluateVectors(left: Vector<any>, right: Vector<any>): Vector<any>;
}

// Math Expressions
export class AddExpression extends BinaryExpression {
    evaluateVectors(left: Vector<any>, right: Vector<any>): Vector<any> {
        // At this point both left and right vectors are guaranteed to be of same type
        // TODO Addition can happen on decimals, big int as well
        // TODO Allow adding other types and if two numbers add upto to big int, change schema
        if (left.getType() !== DataType.Number) {
            throw new SQLError(`Cannot add non-numeric types: ${this.toString()}`);
        }
        const results = new Array<number>();
        for (let i = 0; i < left.getSize(); i++) {
            let assertionResult = left.get(i) as number + right.get(i) as number;
            results.push(assertionResult);
        }

        return new NumberVector(results);
    }
}

// Comparison Expressions
export abstract class BooleanExpression extends BinaryExpression {

    evaluateVectors(left: Vector<any>, right: Vector<any>): Vector<boolean> {
        const results = new Array<boolean>()
        for (let i = 0; i < left.getSize(); i++) {
            let assertionResult = this.compare(left.get(i), right.get(i))
            results.push(assertionResult)
        }

        return new BooleanVector(results);
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
