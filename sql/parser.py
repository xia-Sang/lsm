"""SQL 语法分析器

将词法分析器生成的标记序列解析成抽象语法树(AST)。
"""

from typing import List, Optional

from lexer import Lexer, Token, TokenType
from ast_nodes import (
    BinaryOp, ColumnRef, DeleteStatement, Expression, FunctionCall,
    Identifier, InsertStatement, JoinClause, Literal, OrderByItem,
    SelectStatement, Statement, TableRef, UnaryOp, UpdateStatement
)


class Parser:
    """SQL 语法分析器"""
    
    def __init__(self, lexer: Lexer):
        self.lexer = lexer
        self.current_token = self.lexer.get_next_token()
    
    def error(self, message: str = "语法错误"):
        raise Exception(f"{message} 在第 {self.current_token.line} 行，第 {self.current_token.column} 列")
    
    def eat(self, token_type: TokenType):
        """验证当前标记类型并获取下一个标记"""
        if self.current_token.type == token_type:
            self.current_token = self.lexer.get_next_token()
        else:
            self.error(f"预期标记类型 {token_type}，实际得到 {self.current_token.type}")
    
    def parse(self) -> Statement:
        """解析SQL语句"""
        if self.current_token.type == TokenType.SELECT:
            return self.parse_select()
        elif self.current_token.type == TokenType.INSERT:
            return self.parse_insert()
        elif self.current_token.type == TokenType.UPDATE:
            return self.parse_update()
        elif self.current_token.type == TokenType.DELETE:
            return self.parse_delete()
        else:
            self.error("无效的SQL语句")
    
    def parse_select(self) -> SelectStatement:
        """解析SELECT语句"""
        self.eat(TokenType.SELECT)
        
        # 处理DISTINCT
        distinct = False
        if self.current_token.type == TokenType.DISTINCT:
            distinct = True
            self.eat(TokenType.DISTINCT)
        
        # 解析选择列
        columns = self.parse_column_list()
        
        # 解析FROM子句
        from_table = None
        if self.current_token.type == TokenType.FROM:
            self.eat(TokenType.FROM)
            from_table = self.parse_table_reference()
        
        # 解析WHERE子句
        where = None
        if self.current_token.type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where = self.parse_expression()
        
        # 解析GROUP BY子句
        group_by = None
        if self.current_token.type == TokenType.GROUP:
            self.eat(TokenType.GROUP)
            self.eat(TokenType.BY)
            group_by = self.parse_expression_list()
        
        # 解析HAVING子句
        having = None
        if self.current_token.type == TokenType.HAVING:
            self.eat(TokenType.HAVING)
            having = self.parse_expression()
        
        # 解析ORDER BY子句
        order_by = None
        if self.current_token.type == TokenType.ORDER:
            self.eat(TokenType.ORDER)
            self.eat(TokenType.BY)
            order_by = self.parse_order_by_list()
        
        # 解析LIMIT和OFFSET子句
        limit = None
        offset = None
        if self.current_token.type == TokenType.LIMIT:
            self.eat(TokenType.LIMIT)
            limit = self.parse_expression()
            if self.current_token.type == TokenType.OFFSET:
                self.eat(TokenType.OFFSET)
                offset = self.parse_expression()
        
        return SelectStatement(
            distinct=distinct,
            columns=columns,
            from_table=from_table,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset
        )
    
    def parse_insert(self) -> InsertStatement:
        """解析INSERT语句"""
        self.eat(TokenType.INSERT)
        self.eat(TokenType.INTO)
        
        # 解析表名
        table = self.parse_identifier()
        
        # 解析列名列表
        columns = None
        if self.current_token.type == TokenType.LEFT_PAREN:
            self.eat(TokenType.LEFT_PAREN)
            columns = []
            while True:
                columns.append(self.parse_identifier())
                if self.current_token.type == TokenType.RIGHT_PAREN:
                    break
                self.eat(TokenType.COMMA)
            self.eat(TokenType.RIGHT_PAREN)
        
        # 解析VALUES子句
        self.eat(TokenType.VALUES)
        values = []
        while True:
            self.eat(TokenType.LEFT_PAREN)
            row_values = []
            while True:
                row_values.append(self.parse_expression())
                if self.current_token.type == TokenType.RIGHT_PAREN:
                    break
                self.eat(TokenType.COMMA)
            self.eat(TokenType.RIGHT_PAREN)
            values.append(row_values)
            if self.current_token.type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)
        
        return InsertStatement(table=table, columns=columns, values=values)
    
    def parse_update(self) -> UpdateStatement:
        """解析UPDATE语句"""
        self.eat(TokenType.UPDATE)
        
        # 解析表名
        table = self.parse_identifier()
        
        # 解析SET子句
        self.eat(TokenType.SET)
        set_pairs = []
        while True:
            column = self.parse_identifier()
            self.eat(TokenType.EQUALS)
            value = self.parse_expression()
            set_pairs.append((column, value))
            if self.current_token.type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)
        
        # 解析WHERE子句
        where = None
        if self.current_token.type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where = self.parse_expression()
        
        return UpdateStatement(table=table, set_pairs=set_pairs, where=where)
    
    def parse_delete(self) -> DeleteStatement:
        """解析DELETE语句"""
        self.eat(TokenType.DELETE)
        self.eat(TokenType.FROM)
        
        # 解析表名
        table = self.parse_identifier()
        
        # 解析WHERE子句
        where = None
        if self.current_token.type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where = self.parse_expression()
        
        return DeleteStatement(table=table, where=where)
    
    def parse_expression(self) -> Expression:
        """解析表达式"""
        return self.parse_or_expression()
    
    def parse_or_expression(self) -> Expression:
        """解析OR表达式"""
        expr = self.parse_and_expression()
        
        while self.current_token.type == TokenType.OR:
            operator = self.current_token.value
            self.eat(TokenType.OR)
            right = self.parse_and_expression()
            expr = BinaryOp(left=expr, operator=operator, right=right)
        
        return expr
    
    def parse_and_expression(self) -> Expression:
        """解析AND表达式"""
        expr = self.parse_comparison()
        
        while self.current_token.type == TokenType.AND:
            operator = self.current_token.value
            self.eat(TokenType.AND)
            right = self.parse_comparison()
            expr = BinaryOp(left=expr, operator=operator, right=right)
        
        return expr
    
    def parse_comparison(self) -> Expression:
        """解析比较表达式"""
        expr = self.parse_additive()
        
        while self.current_token.type in (
            TokenType.EQUALS, TokenType.NOT_EQUALS,
            TokenType.LESS, TokenType.LESS_EQUALS,
            TokenType.GREATER, TokenType.GREATER_EQUALS,
            TokenType.LIKE, TokenType.IN, TokenType.BETWEEN
        ):
            operator = self.current_token.value
            self.eat(self.current_token.type)
            
            if operator == 'BETWEEN':
                start = self.parse_additive()
                self.eat(TokenType.AND)
                end = self.parse_additive()
                expr = BinaryOp(
                    left=expr,
                    operator='BETWEEN',
                    right=BinaryOp(left=start, operator='AND', right=end)
                )
            else:
                right = self.parse_additive()
                expr = BinaryOp(left=expr, operator=operator, right=right)
        
        return expr
    
    def parse_additive(self) -> Expression:
        """解析加法表达式"""
        expr = self.parse_multiplicative()
        
        while self.current_token.type in (TokenType.PLUS, TokenType.MINUS):
            operator = self.current_token.value
            self.eat(self.current_token.type)
            right = self.parse_multiplicative()
            expr = BinaryOp(left=expr, operator=operator, right=right)
        
        return expr
    
    def parse_multiplicative(self) -> Expression:
        """解析乘法表达式"""
        expr = self.parse_unary()
        
        while self.current_token.type in (TokenType.MULTIPLY, TokenType.DIVIDE):
            operator = self.current_token.value
            self.eat(self.current_token.type)
            right = self.parse_unary()
            expr = BinaryOp(left=expr, operator=operator, right=right)
        
        return expr
    
    def parse_unary(self) -> Expression:
        """解析一元表达式"""
        if self.current_token.type in (TokenType.PLUS, TokenType.MINUS, TokenType.NOT):
            operator = self.current_token.value
            self.eat(self.current_token.type)
            operand = self.parse_unary()
            return UnaryOp(operator=operator, operand=operand)
        
        return self.parse_primary()
    
    def parse_primary(self) -> Expression:
        """解析基本表达式"""
        token = self.current_token
        
        if token.type == TokenType.NUMBER:
            self.eat(TokenType.NUMBER)
            try:
                value = int(token.value)
            except ValueError:
                value = float(token.value)
            return Literal(value=value)
        
        elif token.type == TokenType.STRING:
            self.eat(TokenType.STRING)
            return Literal(value=token.value)
        
        elif token.type == TokenType.NULL:
            self.eat(TokenType.NULL)
            return Literal(value=None)
        
        elif token.type == TokenType.LEFT_PAREN:
            self.eat(TokenType.LEFT_PAREN)
            expr = self.parse_expression()
            self.eat(TokenType.RIGHT_PAREN)
            return expr
        
        elif token.type in (
            TokenType.IDENTIFIER,
            TokenType.COUNT, TokenType.SUM, TokenType.AVG,
            TokenType.MAX, TokenType.MIN
        ):
            return self.parse_identifier_or_function()
        
        self.error(f"无效的表达式 {token.value}")
    
    def parse_identifier_or_function(self):
        """解析标识符或函数调用"""
        token = self.current_token
        name = token.value
        
        # 处理标识符或聚合函数关键字
        if token.type == TokenType.IDENTIFIER:
            self.eat(TokenType.IDENTIFIER)
        elif token.type in (TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN):
            self.eat(token.type)
        else:
            self.error(f"预期标识符或聚合函数，实际得到 {token.type}")
        
        # 如果下一个token是左括号，这是一个函数调用
        if self.current_token.type == TokenType.LEFT_PAREN:
            self.eat(TokenType.LEFT_PAREN)
            args = []
            
            # 处理特殊情况：COUNT(*)
            if self.current_token.type == TokenType.STAR:
                args.append(Literal(value='*'))
                self.eat(TokenType.STAR)
            else:
                # 解析普通参数列表
                while True:
                    args.append(self.parse_expression())
                    if self.current_token.type != TokenType.COMMA:
                        break
                    self.eat(TokenType.COMMA)
            
            self.eat(TokenType.RIGHT_PAREN)
            
            # 检查是否有 AS 子句
            if self.current_token.type == TokenType.AS:
                self.eat(TokenType.AS)
                alias = self.current_token.value
                self.eat(TokenType.IDENTIFIER)
                return FunctionCall(name=name, args=args, alias=alias)
            
            return FunctionCall(name=name, args=args)
        
        # 如果后面跟着点号，说明是表.列引用
        elif self.current_token.type == TokenType.DOT:
            self.eat(TokenType.DOT)
            column = self.current_token.value
            self.eat(TokenType.IDENTIFIER)
            return ColumnRef(table=name, column=column)
        
        # 否则就是普通标识符
        return Identifier(name=name)
    def parse_identifier(self) -> Identifier:
        """解析标识符"""
        name = self.current_token.value
        self.eat(TokenType.IDENTIFIER)
        return Identifier(name=name)
    
    def parse_column_list(self) -> List[Expression]:
        """解析列名列表"""
        columns = []
        while True:
            if self.current_token.type == TokenType.STAR:
                columns.append(Literal(value='*'))
                self.eat(TokenType.STAR)
            else:
                expr = self.parse_expression()
                
                # 处理列别名
                if self.current_token.type == TokenType.AS:
                    self.eat(TokenType.AS)
                    alias = self.current_token.value
                    self.eat(TokenType.IDENTIFIER)
                    expr = ColumnRef(table=None, column=alias)
                elif self.current_token.type == TokenType.IDENTIFIER:
                    alias = self.current_token.value
                    self.eat(TokenType.IDENTIFIER)
                    expr = ColumnRef(table=None, column=alias)
                
                columns.append(expr)
            
            if self.current_token.type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)
        return columns
    
    def parse_expression_list(self) -> List[Expression]:
        """解析表达式列表"""
        expressions = []
        while True:
            expressions.append(self.parse_expression())
            if self.current_token.type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)
        return expressions
    
    def parse_order_by_list(self) -> List[OrderByItem]:
        """解析ORDER BY列表"""
        items = []
        while True:
            expr = self.parse_expression()
            direction = 'ASC'
            
            if self.current_token.type in (TokenType.ASC, TokenType.DESC):
                direction = self.current_token.value
                self.eat(self.current_token.type)
            
            items.append(OrderByItem(expression=expr, direction=direction))
            
            if self.current_token.type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)
        return items
    
    def parse_table_reference(self) -> Expression:
        """解析表引用"""
        # 解析基本表名或子查询
        if self.current_token.type == TokenType.LEFT_PAREN:
            self.eat(TokenType.LEFT_PAREN)
            table = self.parse_select()
            self.eat(TokenType.RIGHT_PAREN)
        else:
            table = self.parse_identifier()
        
        # 解析表别名
        alias = None
        if self.current_token.type == TokenType.AS:
            self.eat(TokenType.AS)
            alias = self.current_token.value
            self.eat(TokenType.IDENTIFIER)
        elif self.current_token.type == TokenType.IDENTIFIER:
            alias = self.current_token.value
            self.eat(TokenType.IDENTIFIER)
        
        # 解析JOIN子句
        joins = []
        while self.current_token.type in (
            TokenType.JOIN, TokenType.LEFT, TokenType.RIGHT, TokenType.INNER, TokenType.OUTER
        ):
            join_type = 'INNER'
            
            if self.current_token.type == TokenType.LEFT:
                join_type = 'LEFT'
                self.eat(TokenType.LEFT)
                if self.current_token.type == TokenType.OUTER:
                    self.eat(TokenType.OUTER)
            elif self.current_token.type == TokenType.RIGHT:
                join_type = 'RIGHT'
                self.eat(TokenType.RIGHT)
                if self.current_token.type == TokenType.OUTER:
                    self.eat(TokenType.OUTER)
            elif self.current_token.type == TokenType.INNER:
                self.eat(TokenType.INNER)
            
            self.eat(TokenType.JOIN)
            join_table = self.parse_table_reference()
            
            # 解析ON条件
            self.eat(TokenType.ON)
            condition = self.parse_expression()
            
            joins.append(JoinClause(
                join_type=join_type,
                table=join_table,
                condition=condition
            ))
        
        return TableRef(name=table.name, alias=alias, joins=joins)
