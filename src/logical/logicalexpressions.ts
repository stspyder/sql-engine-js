import {LogicalPlan} from './logicalplan';
import {SQLError} from "../errors";
import {BooleanOperator, DataType, MathOperator} from "../types";
import {Field} from "../schema";


export interface LogicalExpression {
    toField(input: LogicalPlan): Promise<Field>;
    toString(): string;
}

export class Column implements LogicalExpression {
    readonly name: string;

    constructor(name: string) {
        this.name = name;
    }

    async toField(input: LogicalPlan): Promise<Field> {
        let schema = await input.getSchema()
        let field = schema.fields.find(field => field.name == this.name);
        if (field) {
            return field;
        } else {
            throw new SQLError("No column named: " + this.name);
        }
    }

    toString(): string {
        return `#${this.name}`;
    }

}


// Literals

export class LiteralString implements LogicalExpression {
    readonly value: string;

    constructor(value: string) {
        this.value = value;
    }

   async toField(input: LogicalPlan): Promise<Field> {
        return new Field(this.value, DataType.String);
   }

    toString(): string {
        return `'${this.value}'`;
    }
}

export class LiteralNumber implements LogicalExpression {
    readonly value: number;

    constructor(value: number) {
        this.value = value;
    }

    async toField(input: LogicalPlan): Promise<Field> {
        return new Field(this.value.toString(), DataType.Number);
    }

    toString(): string {
        return this.value.toString();
    }
}

export class LiteralNull implements LogicalExpression {
    async toField(input: LogicalPlan): Promise<Field> {
        return new Field("null", DataType.NULL);
    }

    toString(): string {
        return `NULL`;
    }
}

//  Binary expressions
export abstract class BinaryExpression implements LogicalExpression {
    protected readonly name: string;
    protected readonly operator: string;
    readonly left: LogicalExpression;
    readonly right: LogicalExpression;

    protected constructor(name: string, operator: string, left: LogicalExpression, right: LogicalExpression) {
        this.name = name;
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    toString(): string {
        return `${this.left} ${this.operator} ${this.right}`;
    }

    abstract toField(input: LogicalPlan): Promise<Field>;
}

// Boolean expressions
export abstract class BooleanBinaryExpression extends BinaryExpression {
    protected constructor(name: string, operator: BooleanOperator, left: LogicalExpression, right: LogicalExpression) {
        super(name, operator.toString(), left, right);
    }

    async toField(input: LogicalPlan): Promise<Field> {
        return new Field(this.name, DataType.Boolean);
    }
}

export class Equals extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("equals", BooleanOperator.Equals, left, right);
    }
}

export class NotEquals extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("not-equals", BooleanOperator.NotEquals, left, right);
    }
}

export class IsNull extends BooleanBinaryExpression {
    constructor(input: LogicalExpression) {
        super("is-null", BooleanOperator.Equals, input, new LiteralNull());
    }
}

export class GreaterThan extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("greater", BooleanOperator.GraterThan, left, right);
    }
}

export class LessThan extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("lesser", BooleanOperator.LessThan, left, right);
    }
}

export class GreaterThanEquals extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("greater-than-equals", BooleanOperator.GreaterThanEquals, left, right);
    }
}

export class LessThanEquals extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("less-than-equals", BooleanOperator.LessThanEquals, left, right);
    }
}

export class And extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("and", BooleanOperator.And, left, right);
    }
}

export class Or extends BooleanBinaryExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("or", BooleanOperator.Or, left, right);
    }
}


// Math Expressions
export abstract class MathExpression extends BinaryExpression {
    protected constructor(name: string, operator: MathOperator, left: LogicalExpression, right: LogicalExpression) {
        super(name, operator.toString(), left, right);
    }

    async toField(input: LogicalPlan): Promise<Field> {
        let inputField = await this.left.toField(input);
        return new Field(this.toString(), inputField.type);
    }
}

export class Add extends MathExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("add", MathOperator.Add, left, right);
    }
}

export class Subtract extends MathExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("subtract", MathOperator.Subtract, left, right);
    }
}

export class Multiply extends MathExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("multiply", MathOperator.Multiply, left, right);
    }
}

export class Divide extends MathExpression {
    constructor(left: LogicalExpression, right: LogicalExpression) {
        super("divide", MathOperator.Divide, left, right);
    }
}

// Aggregate Expressions
export abstract class AggregateExpression implements LogicalExpression {
    protected readonly name: string;
    protected readonly expression: LogicalExpression;

    protected constructor(name: string, expression: LogicalExpression) {
        this.name = name;
        this.expression = expression;
    }

    async toField(input: LogicalPlan): Promise<Field> {
        let inputField = await this.expression.toField(input)
        return new Field(this.name, inputField.type);
    }

    toString(): string {
        return `${this.name}(${this.expression})`
    }
}

export class Sum extends AggregateExpression {
    constructor(expression: LogicalExpression) {
        super("sum", expression);
    }
}

export class Min extends AggregateExpression {
    constructor(expression: LogicalExpression) {
        super("min", expression);
    }
}

export class Max extends AggregateExpression {
    constructor(expression: LogicalExpression) {
        super("max", expression);
    }
}

export class Avg extends AggregateExpression {
    constructor(expression: LogicalExpression) {
        super("avg", expression);
    }
}

export class Count extends AggregateExpression {
    constructor(expression: LogicalExpression) {
        super("count", expression);
    }

    async toField(input: LogicalPlan): Promise<Field> {
        // TODO Convert to BigInt
        return new Field(this.name, DataType.Number);
    }
}

export class Alias implements LogicalExpression {

    private readonly _alias: string;
    private readonly _expression: LogicalExpression

    constructor(alias: string, expression: LogicalExpression) {
        this._alias = alias;
        this._expression = expression;
    }

    async toField(input: LogicalPlan): Promise<Field> {
        let expressionField = await this._expression.toField(input)
        return new Field(this._alias, expressionField.type);
    }

    get expression(): LogicalExpression {
        return this._expression;
    }

    toString(): string {
        return `${this._expression.toString()} as ${this._alias}`
    }
}
