def format_ast(node, indent=0):
    """格式化输出 AST 节点"""
    if node is None:
        return
        
    prefix = "  " * indent
    if isinstance(node, SelectStatement):
        print(f"{prefix}SELECT")
        print(f"{prefix}Columns:")
        for col in node.columns:
            format_ast(col, indent + 1)
        if node.from_table:
            print(f"{prefix}FROM:")
            format_ast(node.from_table, indent + 1)
        if node.where:
            print(f"{prefix}WHERE:")
            format_ast(node.where, indent + 1)
    elif isinstance(node, ColumnRef):
        table_info = f" (Table: {node.table})" if node.table else ""
        print(f"{prefix}Column: {node.column}{table_info}")
    elif isinstance(node, TableRef):
        print(f"{prefix}Table: {node.table}")
    elif isinstance(node, BinaryOp):
        print(f"{prefix}Operation: {node.operator}")
        print(f"{prefix}Left:")
        format_ast(node.left, indent + 1)
        print(f"{prefix}Right:")
        format_ast(node.right, indent + 1)
    elif isinstance(node, Literal):
        print(f"{prefix}Literal: {node.value}")
    elif isinstance(node, FunctionCall):
        print(f"{prefix}Function: {node.name}")
        print(f"{prefix}Arguments:")
        for arg in node.args:
            format_ast(arg, indent + 1)

from lexer import Lexer
from parser import Parser
from ast_nodes import (
    SelectStatement, ColumnRef, BinaryOp, 
    Literal, FunctionCall, TableRef
)

def format_ast(node, indent=0):
    """格式化输出 AST 节点"""
    if node is None:
        return
        
    prefix = "  " * indent
    if isinstance(node, SelectStatement):
        print(f"{prefix}SELECT")
        print(f"{prefix}Columns:")
        for col in node.columns:
            format_ast(col, indent + 1)
        if node.from_table:
            print(f"{prefix}FROM:")
            format_ast(node.from_table, indent + 1)
        if node.where:
            print(f"{prefix}WHERE:")
            format_ast(node.where, indent + 1)
    elif isinstance(node, ColumnRef):
        table_info = f" (Table: {node.table})" if node.table else ""
        print(f"{prefix}Column: {node.column}{table_info}")
    elif isinstance(node, TableRef):
        print(f"{prefix}Table: {node.name}")
    elif isinstance(node, BinaryOp):
        print(f"{prefix}Operation: {node.operator}")
        print(f"{prefix}Left:")
        format_ast(node.left, indent + 1)
        print(f"{prefix}Right:")
        format_ast(node.right, indent + 1)
    elif isinstance(node, Literal):
        print(f"{prefix}Literal: {node.value}")
    elif isinstance(node, FunctionCall):
        print(f"{prefix}Function: {node.name}")
        print(f"{prefix}Arguments:")
        for arg in node.args:
            format_ast(arg, indent + 1)

def parse_and_format(sql):
    """解析 SQL 并格式化输出"""
    print(f"\nSQL: {sql}")
    print("-" * 50)
    lexer = Lexer(sql)
    parser = Parser(lexer)
    ast = parser.parse()
    print(ast)
    print("-" * 50)
    return ast

def main():
    # 示例 1: 简单的 SELECT
    parse_and_format("SELECT id, name FROM users")

    # 示例 2: 带 WHERE 条件的 SELECT
    parse_and_format("""
        SELECT id, name, age 
        FROM users 
        WHERE age > 18
    """)

    # 示例 3: 带聚合函数和别名的复杂 SELECT
    ast=parse_and_format("""
        
        SELECT 
            department,
            COUNT(*) as total_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        WHERE e.salary > 5000 AND d.location = 'NY'
        GROUP BY department
    """)
    from optimizer import QueryOptimizer
    optimizer = QueryOptimizer()
    optimized_ast = optimizer.optimize(ast)
        
    print("优化后的 AST:")
    print(optimized_ast)
    print("-" * 50)
if __name__ == "__main__":
    main()