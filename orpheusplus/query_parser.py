import sqlparse
from sqlparse import keywords
from sqlparse.lexer import Lexer
from sqlparse.sql import Identifier, IdentifierList, Token, Where

from orpheusplus.version_data import DATA_TABLE_SUFFIX, HEAD_SUFFIX
from orpheusplus.version_table import VERSION_TABLE_SUFFIX

lex = Lexer.get_default_instance()
lex.add_keywords({"VTABLE": sqlparse.tokens.Keyword,
                  "VERSION": sqlparse.tokens.Keyword})


class SQLParser():
    def __init__(self):
        self.stmts = []
        self.parsed = []
        self.operations = []
        self._is_modified = []
    
    def parse_file(self, filepath):
        with open(filepath, 'r') as f:
            sql = f.read()
        return self.parse(sql)

    
    def parse(self, sql):
        self.stmts = list(sqlparse.parse(sql))
        self._handle_keywords()
        return self.parsed, self.operations
    
    def _handle_keywords(self):
        # while self.stmts:
        #     stmt = self.stmts.pop(0)
        for stmt in self.stmts:
            tokens = self._strip_whitespace(stmt)
            for idx, token in enumerate(tokens):
                t = token.value.lower()
                if t == "vtable":
                    tokens = self._handle_vtable(tokens, idx)
                    self._is_modified.append(True)
                    break
                elif t == "version":
                    tokens = self._handle_version(tokens)
                    self._is_modified.append(True)
                    break
            self._is_modified.append(False)
            self.operations.append(self._get_operation_type(tokens))
            self.parsed.append(self._rebuild_stmt(tokens))

    @staticmethod           
    def _handle_vtable(tokens, idx):
        """
        e.g., SELECT * FROM VTABLE new_table;
        equiv. to:
        SELECT * FROM new_table_orpheusplus
        WHERE rid IN (
            SELECT * FROM new_table_orpheusplus_version
            WHERE version = {self.version_graph.head}
        )

        e.g., INSERT INTO VTABLE new_table VALUES(...)
        equiv. to:
        INSERT INTO new_table_orpheusplus VALUES(...)
        """
        # Case: > one vtable
        if type(tokens[idx+1]) is IdentifierList:
            pass
        # Case: one vtable with where clause
        elif type(tokens[idx+2]) is Where:
            pass
        # Case: only one vtable and no where clause
        elif type(tokens[idx+1]) is Identifier:
            _ = tokens.pop(idx).value
            table = tokens.pop(idx).value
            replaced = SQLParser._build_identifier(f"{table}{HEAD_SUFFIX}")
            tokens.insert(idx, replaced)

        return tokens
    
    @staticmethod
    def _get_operation_type(tokens):
        return tokens[0].value.lower()

    @staticmethod
    def _build_identifier(content):
        return Identifier([Token("", content)])
    
    @staticmethod
    def _rebuild_stmt(tokens):
        # Check if last is semicolon
        if tokens[-1].match(sqlparse.tokens.Punctuation, ";"):
            tokens.pop()
        stmt = " ".join([token.value for token in tokens]) + ";"
        return stmt

    def _handle_version(self, stmt):
        pass

    @staticmethod
    def _strip_whitespace(stmt):
        return list(filter(lambda token: not token.match(sqlparse.tokens.Whitespace, " "), stmt.tokens))