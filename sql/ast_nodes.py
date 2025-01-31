"""SQL AST 节点定义

定义SQL语句的抽象语法树节点结构。
"""

from dataclasses import dataclass
from typing import List, Optional, Union,Any


@dataclass
class Node:
    """AST 基础节点"""
    pass


@dataclass
class Expression(Node):
    """表达式基类"""
    pass


@dataclass
class Literal(Expression):
    """字面量"""
    value: Union[str, int, float, bool, None]


@dataclass
class Identifier(Expression):
    """标识符"""
    name: str


@dataclass
class BinaryOp(Expression):
    """二元运算"""
    left: Expression
    operator: str
    right: Expression


@dataclass
class UnaryOp(Expression):
    """一元运算"""
    operator: str
    operand: Expression


@dataclass
class FunctionCall:
    name: str
    args: List[Any]
    alias: Optional[str] = None


@dataclass
class ColumnRef(Expression):
    """列引用"""
    table: Optional[str]
    column: str


@dataclass
class Statement(Node):
    """语句基类"""
    pass


@dataclass
class SelectStatement(Statement):
    """SELECT 语句"""
    distinct: bool
    columns: List[Expression]
    from_table: Optional[Expression]
    where: Optional[Expression]
    group_by: Optional[List[Expression]]
    having: Optional[Expression]
    order_by: Optional[List[Expression]]
    limit: Optional[Expression]
    offset: Optional[Expression]


@dataclass
class InsertStatement(Statement):
    """INSERT 语句"""
    table: Identifier
    columns: Optional[List[Identifier]]
    values: List[List[Expression]]


@dataclass
class UpdateStatement(Statement):
    """UPDATE 语句"""
    table: Identifier
    set_pairs: List[tuple[Identifier, Expression]]
    where: Optional[Expression]


@dataclass
class DeleteStatement(Statement):
    """DELETE 语句"""
    table: Identifier
    where: Optional[Expression]


@dataclass
class JoinClause(Node):
    """JOIN 子句"""
    join_type: str  # 'INNER', 'LEFT', 'RIGHT', 'OUTER'
    table: Expression
    condition: Expression


@dataclass
class OrderByItem(Node):
    """ORDER BY 项"""
    expression: Expression
    direction: str  # 'ASC' or 'DESC'


@dataclass
class TableRef(Expression):
    """表引用"""
    name: str
    alias: Optional[str]
    joins: Optional[List[JoinClause]]
