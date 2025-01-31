from typing import Optional
from ast_nodes import *

class QueryOptimizer:
    def optimize(self, ast: SelectStatement) -> SelectStatement:
        """优化查询语句"""
        if not isinstance(ast, SelectStatement):
            return ast
            
        # 1. 谓词下推优化
        ast = self._push_down_predicates(ast)
        
        # 2. 列裁剪优化
        ast = self._prune_columns(ast)
        
        # 3. 常量折叠优化
        ast = self._fold_constants(ast)
        
        return ast
    
    def _push_down_predicates(self, stmt: SelectStatement) -> SelectStatement:
        """谓词下推优化
        将 WHERE 条件尽可能下推到数据扫描层，减少中间结果集
        """
        if not stmt.where:
            return stmt
            
        # 如果有 JOIN，尝试将条件下推到相应的表
        if stmt.from_table and stmt.from_table.joins:
            new_where = []
            pushed_conditions = []
            
            def can_push_down(expr, table_name):
                """检查条件是否只依赖于特定表"""
                if isinstance(expr, ColumnRef):
                    return expr.table == table_name
                elif isinstance(expr, BinaryOp):
                    return (can_push_down(expr.left, table_name) and 
                           can_push_down(expr.right, table_name))
                elif isinstance(expr, Literal):
                    return True
                return False
            
            # 遍历所有 JOIN，将可以下推的条件放到对应的 JOIN ON 子句中
            for join in stmt.from_table.joins:
                if isinstance(stmt.where, BinaryOp) and stmt.where.operator == 'AND':
                    conditions = self._split_and_conditions(stmt.where)
                    for cond in conditions:
                        if can_push_down(cond, join.table.name):
                            pushed_conditions.append(cond)
                            if not join.on:
                                join.on = cond
                            else:
                                join.on = BinaryOp(left=join.on, operator='AND', right=cond)
                        else:
                            new_where.append(cond)
                            
            # 重建 WHERE 子句
            if new_where:
                stmt.where = self._combine_and_conditions(new_where)
            else:
                stmt.where = None
                
        return stmt
    
    def _prune_columns(self, stmt: SelectStatement) -> SelectStatement:
        """列裁剪优化
        只选择查询真正需要的列
        """
        if stmt.columns and len(stmt.columns) == 1 and isinstance(stmt.columns[0], Literal) and stmt.columns[0].value == '*':
            return stmt
            
        needed_columns = set()
        
        # 收集 SELECT 中用到的列
        for col in stmt.columns:
            if isinstance(col, ColumnRef):
                needed_columns.add((col.table, col.column))
            elif isinstance(col, FunctionCall):
                for arg in col.args:
                    if isinstance(arg, ColumnRef):
                        needed_columns.add((arg.table, arg.column))
        
        # 收集 WHERE 中用到的列
        if stmt.where:
            self._collect_columns_from_expr(stmt.where, needed_columns)
        
        # 更新 SELECT 列表
        if needed_columns:
            new_columns = []
            for col in stmt.columns:
                if isinstance(col, ColumnRef):
                    if (col.table, col.column) in needed_columns:
                        new_columns.append(col)
                else:
                    new_columns.append(col)
            stmt.columns = new_columns
            
        return stmt
    
    def _fold_constants(self, stmt: SelectStatement) -> SelectStatement:
        """常量折叠优化
        预计算常量表达式
        """
        def fold_expr(expr):
            if isinstance(expr, BinaryOp):
                # 递归处理左右子表达式
                left = fold_expr(expr.left)
                right = fold_expr(expr.right)
                
                # 如果两边都是常量，进行计算
                if isinstance(left, Literal) and isinstance(right, Literal):
                    try:
                        if expr.operator == '+':
                            return Literal(left.value + right.value)
                        elif expr.operator == '-':
                            return Literal(left.value - right.value)
                        elif expr.operator == '*':
                            return Literal(left.value * right.value)
                        elif expr.operator == '/':
                            return Literal(left.value / right.value)
                    except:
                        pass
                return BinaryOp(left=left, operator=expr.operator, right=right)
            return expr
        
        # 优化 WHERE 子句中的常量表达式
        if stmt.where:
            stmt.where = fold_expr(stmt.where)
            
        # 优化 SELECT 列表中的常量表达式
        new_columns = []
        for col in stmt.columns:
            if isinstance(col, BinaryOp):
                new_columns.append(fold_expr(col))
            else:
                new_columns.append(col)
        stmt.columns = new_columns
        
        return stmt
    
    def _split_and_conditions(self, expr: BinaryOp) -> list:
        """将 AND 连接的条件分解为列表"""
        if not isinstance(expr, BinaryOp) or expr.operator != 'AND':
            return [expr]
        return (self._split_and_conditions(expr.left) + 
                self._split_and_conditions(expr.right))
    
    def _combine_and_conditions(self, conditions: list) -> BinaryOp:
        """将条件列表组合为 AND 表达式"""
        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions[0]
        return BinaryOp(
            left=conditions[0],
            operator='AND',
            right=self._combine_and_conditions(conditions[1:])
        )
    
    def _collect_columns_from_expr(self, expr, columns_set):
        """从表达式中收集列引用"""
        if isinstance(expr, ColumnRef):
            columns_set.add((expr.table, expr.column))
        elif isinstance(expr, BinaryOp):
            self._collect_columns_from_expr(expr.left, columns_set)
            self._collect_columns_from_expr(expr.right, columns_set)
        elif isinstance(expr, FunctionCall):
            for arg in expr.args:
                self._collect_columns_from_expr(arg, columns_set)