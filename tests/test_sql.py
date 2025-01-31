from unittest import TestCase
from sql.lexer import Lexer, TokenType
from sql.parser import (
    Parser, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
    Column, WhereClause, Condition, JoinClause, OrderByItem, GroupByClause,
    AggregateFunction, SubqueryExpression, BetweenExpression, LikeExpression,
    IsNullExpression
)
import tempfile
import shutil
import os


class TestLexer(TestCase):
    """测试词法分析器"""
    
    def test_select(self):
        """测试 SELECT 的词法分析"""
        sql = "SELECT id, name FROM users WHERE age >= 18;"
        lexer = Lexer(sql)
        
        tokens = []
        token = lexer.get_next_token()
        while token.type != TokenType.EOF:
            tokens.append(token)
            token = lexer.get_next_token()
        
        expected_types = [
            TokenType.SELECT,
            TokenType.IDENTIFIER, TokenType.COMMA,
            TokenType.IDENTIFIER,
            TokenType.FROM,
            TokenType.IDENTIFIER,
            TokenType.WHERE,
            TokenType.IDENTIFIER,
            TokenType.GREATER_EQUALS,
            TokenType.NUMBER,
            TokenType.SEMICOLON
        ]
        
        self.assertEqual(len(tokens), len(expected_types))
        for token, expected_type in zip(tokens, expected_types):
            self.assertEqual(token.type, expected_type)


class TestParser(TestCase):
    """测试语法分析器"""
    
    def test_select(self):
        """测试 SELECT 语法分析"""
        sql = "SELECT id, name FROM users WHERE age >= 18;"
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, SelectStatement)
        self.assertEqual(len(ast.columns), 2)
        self.assertEqual(ast.columns[0].name, 'id')
        self.assertEqual(ast.columns[1].name, 'name')
        self.assertEqual(ast.table, 'users')
        self.assertIsInstance(ast.where, WhereClause)
        self.assertEqual(len(ast.where.conditions), 1)
        self.assertEqual(ast.where.conditions[0].left, 'age')
        self.assertEqual(ast.where.conditions[0].operator, '>=')
        self.assertEqual(ast.where.conditions[0].right, 18.0)
    
    def test_insert(self):
        """测试 INSERT 语法分析"""
        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20);"
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, InsertStatement)
        self.assertEqual(ast.table, 'users')
        self.assertEqual(ast.columns, ['id', 'name', 'age'])
        self.assertEqual(ast.values, [1.0, 'Alice', 20.0])
    
    def test_update(self):
        """测试 UPDATE 语法分析"""
        sql = "UPDATE users SET name = 'Bob', age = 25 WHERE id = 1;"
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, UpdateStatement)
        self.assertEqual(ast.table, 'users')
        self.assertEqual(ast.set_pairs, [('name', 'Bob'), ('age', 25.0)])
        self.assertIsInstance(ast.where, WhereClause)
        self.assertEqual(len(ast.where.conditions), 1)
        self.assertEqual(ast.where.conditions[0].left, 'id')
        self.assertEqual(ast.where.conditions[0].operator, '=')
        self.assertEqual(ast.where.conditions[0].right, 1.0)
    
    def test_delete(self):
        """测试 DELETE 语法分析"""
        sql = "DELETE FROM users WHERE id = 1;"
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, DeleteStatement)
        self.assertEqual(ast.table, 'users')
        self.assertIsInstance(ast.where, WhereClause)
        self.assertEqual(len(ast.where.conditions), 1)
        self.assertEqual(ast.where.conditions[0].left, 'id')
        self.assertEqual(ast.where.conditions[0].operator, '=')
        self.assertEqual(ast.where.conditions[0].right, 1.0)
    
    def test_complex_select(self):
        """测试复杂的 SELECT 语句"""
        sql = """
        SELECT 
            COUNT(u.id) as user_count,
            d.name as department,
            AVG(u.age) as avg_age,
            MIN(u.score) as min_score
        FROM users u
        LEFT JOIN departments d ON u.department_id = d.id
        WHERE u.age >= 18
        GROUP BY d.name
        HAVING COUNT(u.id) > 5
        ORDER BY avg_age DESC, department ASC
        LIMIT 10 OFFSET 20;
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, SelectStatement)
        
        # 验证列
        self.assertEqual(len(ast.columns), 4)
        
        # COUNT(u.id) as user_count
        self.assertEqual(ast.columns[0].name, 'id')
        self.assertEqual(ast.columns[0].table, 'u')
        self.assertEqual(ast.columns[0].alias, 'user_count')
        self.assertEqual(ast.columns[0].aggregate.func, 'COUNT')
        
        # d.name as department
        self.assertEqual(ast.columns[1].name, 'name')
        self.assertEqual(ast.columns[1].table, 'd')
        self.assertEqual(ast.columns[1].alias, 'department')
        
        # AVG(u.age) as avg_age
        self.assertEqual(ast.columns[2].name, 'age')
        self.assertEqual(ast.columns[2].table, 'u')
        self.assertEqual(ast.columns[2].alias, 'avg_age')
        self.assertEqual(ast.columns[2].aggregate.func, 'AVG')
        
        # MIN(u.score) as min_score
        self.assertEqual(ast.columns[3].name, 'score')
        self.assertEqual(ast.columns[3].table, 'u')
        self.assertEqual(ast.columns[3].alias, 'min_score')
        self.assertEqual(ast.columns[3].aggregate.func, 'MIN')
        
        # 验证表和连接
        self.assertEqual(ast.table, 'users')
        self.assertEqual(len(ast.joins), 1)
        self.assertEqual(ast.joins[0].type, 'LEFT')
        self.assertEqual(ast.joins[0].table, 'departments')
        
        # 验证 WHERE 子句
        self.assertIsInstance(ast.where, WhereClause)
        self.assertEqual(len(ast.where.conditions), 1)
        self.assertEqual(ast.where.conditions[0].left, 'u.age')
        self.assertEqual(ast.where.conditions[0].operator, '>=')
        self.assertEqual(ast.where.conditions[0].right, 18.0)
        
        # 验证 GROUP BY 子句
        self.assertIsInstance(ast.group_by, GroupByClause)
        self.assertEqual(ast.group_by.columns, ['d.name'])
        self.assertIsInstance(ast.group_by.having, WhereClause)
        
        # 验证 ORDER BY 子句
        self.assertEqual(len(ast.order_by), 2)
        self.assertEqual(ast.order_by[0].column, 'avg_age')
        self.assertEqual(ast.order_by[0].direction, 'DESC')
        self.assertEqual(ast.order_by[1].column, 'department')
        self.assertEqual(ast.order_by[1].direction, 'ASC')
        
        # 验证 LIMIT 和 OFFSET
        self.assertEqual(ast.limit, 10)
        self.assertEqual(ast.offset, 20)
    
    def test_simple_join(self):
        """测试简单的 JOIN 语句"""
        sql = """
        SELECT a.id, a.name, b.age 
        FROM table_a a 
        INNER JOIN table_b b ON a.id = b.id;
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, SelectStatement)
        self.assertEqual(len(ast.columns), 3)
        self.assertEqual(ast.table, 'table_a')
        self.assertEqual(len(ast.joins), 1)
        self.assertEqual(ast.joins[0].type, 'INNER')
        self.assertEqual(ast.joins[0].table, 'table_b')
    
    def test_aggregate_functions(self):
        """测试聚合函数"""
        sql = """
        SELECT 
            COUNT(*) as total,
            AVG(age) as avg_age,
            MAX(score) as max_score,
            MIN(score) as min_score
        FROM users;
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, SelectStatement)
        self.assertEqual(len(ast.columns), 4)
        
        # COUNT(*)
        self.assertEqual(ast.columns[0].name, '*')
        self.assertEqual(ast.columns[0].alias, 'total')
        self.assertEqual(ast.columns[0].aggregate.func, 'COUNT')
        
        # AVG(age)
        self.assertEqual(ast.columns[1].name, 'age')
        self.assertEqual(ast.columns[1].alias, 'avg_age')
        self.assertEqual(ast.columns[1].aggregate.func, 'AVG')
        
        # MAX(score)
        self.assertEqual(ast.columns[2].name, 'score')
        self.assertEqual(ast.columns[2].alias, 'max_score')
        self.assertEqual(ast.columns[2].aggregate.func, 'MAX')
        
        # MIN(score)
        self.assertEqual(ast.columns[3].name, 'score')
        self.assertEqual(ast.columns[3].alias, 'min_score')
        self.assertEqual(ast.columns[3].aggregate.func, 'MIN')
    
    def test_union(self):
        """测试 UNION 操作"""
        sql = """
        SELECT id, name FROM users WHERE age >= 18
        UNION
        SELECT id, name FROM employees WHERE salary >= 5000
        UNION ALL
        SELECT id, name FROM contractors WHERE status = 'active';
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, SelectStatement)
        self.assertEqual(len(ast.unions), 2)
        
        # 第一个 UNION
        self.assertFalse(ast.unions[0].all)  # UNION
        self.assertEqual(ast.unions[0].query.table, 'employees')
        
        # 第二个 UNION
        self.assertTrue(ast.unions[1].all)  # UNION ALL
        self.assertEqual(ast.unions[1].query.table, 'contractors')
    
    def test_subquery(self):
        """测试子查询"""
        sql = """
        SELECT id, name 
        FROM users 
        WHERE department_id IN (
            SELECT id 
            FROM departments 
            WHERE location = 'New York'
        );
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertIsInstance(ast, SelectStatement)
        where_condition = ast.where.conditions[0]
        self.assertIsInstance(where_condition, SubqueryExpression)
        self.assertEqual(where_condition.operator, 'IN')
        self.assertEqual(where_condition.query.table, 'departments')
    
    def test_between(self):
        """测试 BETWEEN 操作"""
        sql = "SELECT * FROM products WHERE price BETWEEN 10.0 AND 20.0;"
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        where_condition = ast.where.conditions[0]
        self.assertIsInstance(where_condition, BetweenExpression)
        self.assertEqual(where_condition.column, 'price')
        self.assertEqual(where_condition.start, 10.0)
        self.assertEqual(where_condition.end, 20.0)
    
    def test_like(self):
        """测试 LIKE 操作"""
        sql = "SELECT * FROM users WHERE name LIKE '%John%';"
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        where_condition = ast.where.conditions[0]
        self.assertIsInstance(where_condition, LikeExpression)
        self.assertEqual(where_condition.column, 'name')
        self.assertEqual(where_condition.pattern, '%John%')
    
    def test_is_null(self):
        """测试 IS NULL 操作"""
        sql = """
        SELECT * FROM users 
        WHERE phone IS NULL 
        AND email IS NOT NULL;
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        self.assertEqual(len(ast.where.conditions), 2)
        
        # IS NULL
        condition1 = ast.where.conditions[0]
        self.assertIsInstance(condition1, IsNullExpression)
        self.assertEqual(condition1.column, 'phone')
        self.assertFalse(condition1.is_not)
        
        # IS NOT NULL
        condition2 = ast.where.conditions[1]
        self.assertIsInstance(condition2, IsNullExpression)
        self.assertEqual(condition2.column, 'email')
        self.assertTrue(condition2.is_not)
    
    def test_complex_query(self):
        """测试复杂查询"""
        sql = """
        SELECT 
            u.id,
            u.name,
            d.name as department
        FROM users u
        LEFT JOIN departments d ON u.department_id = d.id
        WHERE 
            u.age BETWEEN 25 AND 35
            AND u.status = 'active'
            AND u.role IN (
                SELECT role_id 
                FROM roles 
                WHERE permission_level >= 3
            )
            AND u.manager_id IS NOT NULL
            AND u.name LIKE '%Smith%'
        ORDER BY u.name ASC
        LIMIT 10;
        """
        
        lexer = Lexer(sql)
        parser = Parser(lexer)
        ast = parser.parse()
        
        # 验证基本结构
        self.assertEqual(len(ast.columns), 3)
        self.assertEqual(len(ast.joins), 1)
        self.assertEqual(len(ast.where.conditions), 5)
        self.assertEqual(len(ast.order_by), 1)
        self.assertEqual(ast.limit, 10)
        
        # 验证条件类型
        conditions = ast.where.conditions
        self.assertIsInstance(conditions[0], BetweenExpression)
        self.assertIsInstance(conditions[1], Condition)
        self.assertIsInstance(conditions[2], SubqueryExpression)
        self.assertIsInstance(conditions[3], IsNullExpression)
        self.assertIsInstance(conditions[4], LikeExpression)




if __name__ == '__main__':
    unittest.main()
