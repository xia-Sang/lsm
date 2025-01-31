"""SQL 词法分析器

负责将 SQL 语句分解成一系列标记（tokens）。
"""

from enum import Enum


class TokenType(Enum):
    """SQL 标记类型"""
    # 关键字
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    FROM = 'FROM'
    WHERE = 'WHERE'
    INTO = 'INTO'
    VALUES = 'VALUES'
    SET = 'SET'
    JOIN = 'JOIN'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    INNER = 'INNER'
    OUTER = 'OUTER'
    ON = 'ON'
    GROUP = 'GROUP'
    BY = 'BY'
    HAVING = 'HAVING'
    ORDER = 'ORDER'
    ASC = 'ASC'
    DESC = 'DESC'
    LIMIT = 'LIMIT'
    OFFSET = 'OFFSET'
    AND = 'AND'
    OR = 'OR'
    UNION = 'UNION'
    ALL = 'ALL'
    EXISTS = 'EXISTS'
    NOT = 'NOT'
    IN = 'IN'
    BETWEEN = 'BETWEEN'
    LIKE = 'LIKE'
    IS = 'IS'
    NULL = 'NULL'
    AS = 'AS'
    DISTINCT = 'DISTINCT'
    # 聚合函数
    COUNT = 'COUNT'
    SUM = 'SUM'
    AVG = 'AVG'
    MAX = 'MAX'
    MIN = 'MIN'
    # 运算符
    PLUS = '+'
    MINUS = '-'
    MULTIPLY = '*'
    DIVIDE = '/'
    EQUALS = '='
    GREATER = '>'
    LESS = '<'
    GREATER_EQUALS = '>='
    LESS_EQUALS = '<='
    NOT_EQUALS = '!='
    DOT = '.'
    COMMA = ','
    SEMICOLON = ';'
    LEFT_PAREN = '('
    RIGHT_PAREN = ')'
    STAR = '*'
    # 其他
    IDENTIFIER = 'IDENTIFIER'
    STRING = 'STRING'
    NUMBER = 'NUMBER'
    EOF = 'EOF'


class Token:
    """SQL 标记"""
    def __init__(self, type_: TokenType, value: str, line: int, column: int):
        self.type = type_
        self.value = value
        self.line = line
        self.column = column
    
    def __str__(self):
        return f"Token({self.type}, '{self.value}', line={self.line}, column={self.column})"


class Lexer:
    """SQL 词法分析器"""
    
    KEYWORDS = {
        'SELECT': TokenType.SELECT,
        'INSERT': TokenType.INSERT,
        'UPDATE': TokenType.UPDATE,
        'DELETE': TokenType.DELETE,
        'FROM': TokenType.FROM,
        'WHERE': TokenType.WHERE,
        'INTO': TokenType.INTO,
        'VALUES': TokenType.VALUES,
        'SET': TokenType.SET,
        'JOIN': TokenType.JOIN,
        'LEFT': TokenType.LEFT,
        'RIGHT': TokenType.RIGHT,
        'INNER': TokenType.INNER,
        'OUTER': TokenType.OUTER,
        'ON': TokenType.ON,
        'GROUP': TokenType.GROUP,
        'BY': TokenType.BY,
        'HAVING': TokenType.HAVING,
        'ORDER': TokenType.ORDER,
        'ASC': TokenType.ASC,
        'DESC': TokenType.DESC,
        'LIMIT': TokenType.LIMIT,
        'OFFSET': TokenType.OFFSET,
        'AND': TokenType.AND,
        'OR': TokenType.OR,
        'UNION': TokenType.UNION,
        'ALL': TokenType.ALL,
        'EXISTS': TokenType.EXISTS,
        'NOT': TokenType.NOT,
        'IN': TokenType.IN,
        'BETWEEN': TokenType.BETWEEN,
        'LIKE': TokenType.LIKE,
        'IS': TokenType.IS,
        'NULL': TokenType.NULL,
        'AS': TokenType.AS,
        'DISTINCT': TokenType.DISTINCT,
        'COUNT': TokenType.COUNT,
        'SUM': TokenType.SUM,
        'AVG': TokenType.AVG,
        'MAX': TokenType.MAX,
        'MIN': TokenType.MIN,
    }
    
    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1
        self.current_char = text[0] if text else None
    
    def error(self):
        raise Exception(f'非法字符 {self.current_char} 在第 {self.line} 行，第 {self.column} 列')
    
    def advance(self):
        """移动到下一个字符"""
        self.pos += 1
        if self.pos > len(self.text) - 1:
            self.current_char = None
        else:
            if self.text[self.pos] == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.current_char = self.text[self.pos]
    
    def skip_whitespace(self):
        """跳过空白字符"""
        while self.current_char and self.current_char.isspace():
            if self.current_char == '\n':
                self.line += 1
                self.column = 0
            self.advance()
    
    def skip_comment(self):
        """跳过注释"""
        while self.current_char and self.current_char != '\n':
            self.advance()
        if self.current_char == '\n':
            self.advance()
    
    def get_identifier(self):
        """获取标识符或关键字"""
        result = ''
        column = self.column
        
        while self.current_char and (self.current_char.isalnum() or self.current_char == '_'):
            result += self.current_char
            self.advance()
        
        token_type = self.KEYWORDS.get(result.upper(), TokenType.IDENTIFIER)
        return Token(token_type, result, self.line, column)
    
    def get_number(self):
        """获取数字"""
        result = ''
        column = self.column
        
        # 处理负号
        if self.current_char == '-':
            result += self.current_char
            self.advance()
        
        # 处理整数部分
        while self.current_char and self.current_char.isdigit():
            result += self.current_char
            self.advance()
        
        # 处理小数点和小数部分
        if self.current_char == '.':
            result += self.current_char
            self.advance()
            while self.current_char and self.current_char.isdigit():
                result += self.current_char
                self.advance()
        
        # 处理科学计数法
        if self.current_char in ('e', 'E'):
            result += self.current_char
            self.advance()
            if self.current_char in ('+', '-'):
                result += self.current_char
                self.advance()
            while self.current_char and self.current_char.isdigit():
                result += self.current_char
                self.advance()
        
        return Token(TokenType.NUMBER, result, self.line, column)
    
    def get_string(self):
        """获取字符串字面量"""
        result = ''
        column = self.column
        quote = self.current_char  # 记录是单引号还是双引号
        self.advance()  # 跳过开始的引号
        
        while self.current_char and self.current_char != quote:
            if self.current_char == '\\':
                self.advance()
                if self.current_char == 'n':
                    result += '\n'
                elif self.current_char == 't':
                    result += '\t'
                elif self.current_char == 'r':
                    result += '\r'
                else:
                    result += self.current_char
            else:
                result += self.current_char
            self.advance()
        
        if self.current_char != quote:
            raise Exception(f'未闭合的字符串在第 {self.line} 行')
        
        self.advance()  # 跳过结束的引号
        return Token(TokenType.STRING, result, self.line, column)
    
    def peek(self) -> str:
        """查看下一个字符"""
        peek_pos = self.pos + 1
        if peek_pos > len(self.text) - 1:
            return None
        return self.text[peek_pos]
    
    def get_next_token(self) -> Token:
        """获取下一个标记"""
        while self.current_char:
            
            # 跳过空白字符
            if self.current_char.isspace():
                self.skip_whitespace()
                continue
            
            # 跳过单行注释
            if self.current_char == '-' and self.peek() == '-':
                self.advance()
                self.advance()
                self.skip_comment()
                continue
            
            # 跳过多行注释
            if self.current_char == '/' and self.peek() == '*':
                self.advance()
                self.advance()
                while self.current_char:
                    if self.current_char == '*' and self.peek() == '/':
                        self.advance()
                        self.advance()
                        break
                    self.advance()
                continue
            
            # 标识符
            if self.current_char.isalpha() or self.current_char == '_':
                return self.get_identifier()
            
            # 数字
            if self.current_char.isdigit() or (self.current_char == '-' and self.peek().isdigit()):
                return self.get_number()
            
            # 字符串
            if self.current_char in ("'", '"'):
                return self.get_string()
            
            # 运算符
            if self.current_char == '+':
                column = self.column
                self.advance()
                return Token(TokenType.PLUS, '+', self.line, column)
            
            if self.current_char == '-':
                column = self.column
                self.advance()
                return Token(TokenType.MINUS, '-', self.line, column)
            
            if self.current_char == '*':
                column = self.column
                self.advance()
                if self.current_char == '*':
                    self.advance()
                    return Token(TokenType.STAR, '**', self.line, column)
                return Token(TokenType.MULTIPLY, '*', self.line, column)
            
            if self.current_char == '/':
                column = self.column
                self.advance()
                return Token(TokenType.DIVIDE, '/', self.line, column)
            
            if self.current_char == '=':
                column = self.column
                self.advance()
                return Token(TokenType.EQUALS, '=', self.line, column)
            
            if self.current_char == '>':
                column = self.column
                self.advance()
                if self.current_char == '=':
                    self.advance()
                    return Token(TokenType.GREATER_EQUALS, '>=', self.line, column)
                return Token(TokenType.GREATER, '>', self.line, column)
            
            if self.current_char == '<':
                column = self.column
                self.advance()
                if self.current_char == '=':
                    self.advance()
                    return Token(TokenType.LESS_EQUALS, '<=', self.line, column)
                return Token(TokenType.LESS, '<', self.line, column)
            
            if self.current_char == '!':
                column = self.column
                self.advance()
                if self.current_char == '=':
                    self.advance()
                    return Token(TokenType.NOT_EQUALS, '!=', self.line, column)
                self.error()
            
            if self.current_char == '.':
                column = self.column
                self.advance()
                return Token(TokenType.DOT, '.', self.line, column)
            
            if self.current_char == ',':
                column = self.column
                self.advance()
                return Token(TokenType.COMMA, ',', self.line, column)
            
            if self.current_char == ';':
                column = self.column
                self.advance()
                return Token(TokenType.SEMICOLON, ';', self.line, column)
            
            if self.current_char == '(':
                column = self.column
                self.advance()
                return Token(TokenType.LEFT_PAREN, '(', self.line, column)
            
            if self.current_char == ')':
                column = self.column
                self.advance()
                return Token(TokenType.RIGHT_PAREN, ')', self.line, column)
            
            self.error()
        
        return Token(TokenType.EOF, '', self.line, self.column)
