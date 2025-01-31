"""SQL 解析器测试"""

import unittest
from lexer import Lexer
from parser import Parser
from ast_nodes import (
    SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
    BinaryOp, ColumnRef, Identifier, Literal, TableRef, JoinClause
)


class TestParser(unittest.TestCase):
    """SQL 解析器测试类"""

    def parse_sql(self, sql: str) -> Parser:
        """辅助方法：创建解析器并解析SQL"""
        lexer = Lexer(sql)
        parser = Parser(lexer)
        return parser

    def test_select_simple(self):
        """测试简单的 SELECT 语句"""
        sql = "SELECT id, name FROM users"
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, SelectStatement)
        self.assertEqual(len(stmt.columns), 2)
        self.assertIsInstance(stmt.columns[0], Identifier)
        self.assertEqual(stmt.columns[0].name, "id")
        self.assertIsInstance(stmt.columns[1], Identifier)
        self.assertEqual(stmt.columns[1].name, "name")
        self.assertIsInstance(stmt.from_table, TableRef)
        self.assertEqual(stmt.from_table.name, "users")

    def test_select_with_where(self):
        """测试带 WHERE 子句的 SELECT 语句"""
        sql = "SELECT * FROM users WHERE age > 18"
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, SelectStatement)
        self.assertEqual(len(stmt.columns), 1)
        self.assertIsInstance(stmt.columns[0], Literal)
        self.assertEqual(stmt.columns[0].value, "*")
        self.assertIsInstance(stmt.where, BinaryOp)
        self.assertEqual(stmt.where.operator, ">")
        self.assertIsInstance(stmt.where.left, Identifier)
        self.assertEqual(stmt.where.left.name, "age")
        self.assertIsInstance(stmt.where.right, Literal)
        self.assertEqual(stmt.where.right.value, 18)

    def test_select_with_join(self):
        """测试带 JOIN 的 SELECT 语句"""
        sql = """
        SELECT u.id, u.name, o.order_id
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        """
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, SelectStatement)
        self.assertEqual(len(stmt.columns), 3)
        self.assertIsInstance(stmt.from_table, TableRef)
        self.assertEqual(stmt.from_table.name, "users")
        self.assertEqual(stmt.from_table.alias, "u")
        self.assertEqual(len(stmt.from_table.joins), 1)
        
        join = stmt.from_table.joins[0]
        self.assertIsInstance(join, JoinClause)
        self.assertEqual(join.join_type, "LEFT")
        self.assertIsInstance(join.table, TableRef)
        self.assertEqual(join.table.name, "orders")
        self.assertEqual(join.table.alias, "o")

    def test_insert(self):
        """测试 INSERT 语句"""
        sql = """
        INSERT INTO users (id, name, age)
        VALUES (1, 'John', 25)
        """
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, InsertStatement)
        self.assertEqual(stmt.table.name, "users")
        self.assertEqual(len(stmt.columns), 3)
        self.assertEqual([col.name for col in stmt.columns], ["id", "name", "age"])
        self.assertEqual(len(stmt.values), 1)
        self.assertEqual(len(stmt.values[0]), 3)
        self.assertEqual(stmt.values[0][0].value, 1)
        self.assertEqual(stmt.values[0][1].value, "John")
        self.assertEqual(stmt.values[0][2].value, 25)

    def test_update(self):
        """测试 UPDATE 语句"""
        sql = """
        UPDATE users
        SET name = 'John', age = 26
        WHERE id = 1
        """
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, UpdateStatement)
        self.assertEqual(stmt.table.name, "users")
        self.assertEqual(len(stmt.set_pairs), 2)
        self.assertEqual(stmt.set_pairs[0][0].name, "name")
        self.assertEqual(stmt.set_pairs[0][1].value, "John")
        self.assertEqual(stmt.set_pairs[1][0].name, "age")
        self.assertEqual(stmt.set_pairs[1][1].value, 26)
        self.assertIsInstance(stmt.where, BinaryOp)
        self.assertEqual(stmt.where.operator, "=")
        self.assertEqual(stmt.where.right.value, 1)

    def test_delete(self):
        """测试 DELETE 语句"""
        sql = "DELETE FROM users WHERE id = 1"
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, DeleteStatement)
        self.assertEqual(stmt.table.name, "users")
        self.assertIsInstance(stmt.where, BinaryOp)
        self.assertEqual(stmt.where.operator, "=")
        self.assertEqual(stmt.where.right.value, 1)

    def test_complex_select(self):
        """测试复杂的 SELECT 语句"""
        sql = """
        SELECT DISTINCT 
            u.id,
            u.name,
            COUNT(*) as total_orders,
            SUM(o.amount) as total_amount
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
            AND o.created_at >= '2024-01-01'
        GROUP BY u.id, u.name
        HAVING COUNT(*) > 5
        ORDER BY total_amount DESC
        LIMIT 10 OFFSET 0
        """
        parser = self.parse_sql(sql)
        stmt = parser.parse()

        self.assertIsInstance(stmt, SelectStatement)
        self.assertTrue(stmt.distinct)
        self.assertEqual(len(stmt.columns), 4)
        self.assertIsInstance(stmt.from_table, TableRef)
        self.assertEqual(stmt.from_table.name, "users")
        self.assertEqual(stmt.from_table.alias, "u")
        self.assertIsInstance(stmt.where, BinaryOp)
        self.assertEqual(stmt.where.operator, "AND")
        self.assertEqual(len(stmt.group_by), 2)
        self.assertIsInstance(stmt.having, BinaryOp)
        self.assertEqual(len(stmt.order_by), 1)
        self.assertEqual(stmt.order_by[0].direction, "DESC")
        self.assertIsInstance(stmt.limit, Literal)
        self.assertEqual(stmt.limit.value, 10)
        self.assertIsInstance(stmt.offset, Literal)
        self.assertEqual(stmt.offset.value, 0)


if __name__ == '__main__':
    unittest.main()
