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
        self.is_modified = []
    
    def parse_file(self, filepath):
        with open(filepath, 'r') as f:
            sql = f.read()
        return self.parse(sql)

    
    def parse(self, sql):
        self.stmts = list(sqlparse.parse(sql))
        self._parse_stmts()
        return self.parsed, self.operations
    
    def _parse_stmts(self):
        for stmt in self.stmts:
            tokens = self._strip_whitespace(stmt.tokens)
            parsed_tokens = self._handle_keywords(tokens)
            if tokens == parsed_tokens:
                self.is_modified.append(False)
            else:
                self.is_modified.append(True)
            self.operations.append(self._get_operation_type(tokens))
            self.parsed.append(self._rebuild_stmt(tokens))

    @staticmethod           
    def _handle_keywords(tokens):
        """
        e.g., SELECT * FROM VTABLE new_table;
        equiv. to:
        SELECT * FROM new_table_orpheusplus
        WHERE rid IN (
            SELECT rid FROM new_table{VERSION_TABLE_SUFFIX}
            WHERE version = {self.version_graph.head}
        )

        e.g., INSERT INTO VTABLE new_table VALUES(...)
        equiv. to:
        INSERT INTO new_table_orpheusplus VALUES(...)
        """
        vtable_indices = []
        of_indices = []
        version_indices = []
        for idx, token in enumerate(tokens):
            if token.match(sqlparse.tokens.Keyword, "VTABLE"):
                vtable_indices.append(idx)
            elif token.match(sqlparse.tokens.Keyword, "OF"):
                of_indices.append(idx)
            elif token.match(sqlparse.tokens.Keyword, "VERSION"):
                version_indices.append(idx)
        SQLParser._check_syntax(tokens, vtable_indices, of_indices, version_indices)

        if version_indices:
            tokens = SQLParser._handle_version(tokens, version_indices)
        tokens = SQLParser._handle_vtable(tokens)

        return tokens
                
    @staticmethod
    def _check_syntax(tokens, vtable_indices, of_indices, version_indices):
        # Only "VTABLE one_table_name OF VERSION version" is valid
        for vtable_idx in vtable_indices:
            if tokens[vtable_idx + 2].match(sqlparse.tokens.Keyword, "OF"):
                if not tokens[vtable_idx + 3].match(sqlparse.tokens.Keyword, "VERSION"):
                    raise SyntaxError("The valid syntax is `VTABLE one_table_name OF VERSION version`")
                if not tokens[vtable_idx + 4].ttype is sqlparse.tokens.Literal.Number.Integer:
                    raise SyntaxError("Require a version number after `VERSION`")

        # possible_version_idices = [idx+3 for idx in vtable_indices]
        # for version_idx in version_indices:
        #     if version_idx not in possible_version_idices:
        #         raise SyntaxError("The valid syntax is `VTABLE one_table_name OF VERSION version`")
        #     elif not tokens[version_idx + 1].ttype is sqlparse.tokens.Literal.Number.Integer:
        #         raise SyntaxError("Require a version number after `VERSION`")
    
    @staticmethod
    def _handle_version(tokens, indices):
        where_indices = SQLParser._get_where(tokens)
        for idx in indices:
            vtable_idx = idx - 3
            table_name_idx = idx - 2
            of_idx = idx - 1
            version_num_idx= idx + 1
            replace = (
                f"{tokens[table_name_idx].value}{DATA_TABLE_SUFFIX} "
                f"WHERE rid IN ("
                f"SELECT rid FROM {tokens[table_name_idx].value}{VERSION_TABLE_SUFFIX} "
                f"WHERE version = {tokens[idx + 1]})"
            )
            
            for where_idx in where_indices:
                # 0: WHERE, 1: WhiteSpace
                replace += " AND "
                replace += " ".join([token.value for token in tokens[where_idx].tokens[2:]])
                # Strip the trailing semicolon if exists
                replace = replace.strip(" ;")

            tokens[vtable_idx] = SQLParser._empty_identifier()
            tokens[table_name_idx] = SQLParser._empty_identifier()
            tokens[of_idx] = SQLParser._empty_identifier()
            tokens[idx] = SQLParser._build_identifier(replace)
            tokens[version_num_idx] = SQLParser._empty_identifier()
        
        for where_idx in where_indices:
            tokens[where_idx] = SQLParser._empty_identifier()

        return tokens

    @staticmethod
    def _handle_vtable(tokens):
        indices = []
        for idx, token in enumerate(tokens):
            if token.match(sqlparse.tokens.Keyword, "VTABLE"):
                indices.append(idx)

        for idx in indices:
            # Case: > one vtable
            if type(tokens[idx + 1]) is IdentifierList:
                replace = []
                table_names = tokens[idx + 1].value.split(",")
                for table_name in table_names:
                    table_name = table_name.strip()
                    replace.append(f"{table_name}{HEAD_SUFFIX}")
                tokens[idx] = SQLParser._empty_identifier()
                tokens[idx + 1] = SQLParser._build_identifier(", ".join(replace))
            # Case: only one vtable
            elif type(tokens[idx + 1]) is Identifier:
                replace = SQLParser._build_identifier(f"{tokens[idx + 1]}{HEAD_SUFFIX}")
                tokens[idx] = SQLParser._empty_identifier()
                tokens[idx + 1] = replace

        return tokens
    
    @staticmethod
    def _get_where(tokens):
        indices = []
        for idx, token in enumerate(tokens):
            if type(token) is sqlparse.sql.Where:
                indices.append(idx)
        return indices
    
    @staticmethod
    def _get_operation_type(tokens):
        return tokens[0].value.lower()

    @staticmethod
    def _build_identifier(content):
        return Identifier([Token("", content)])
    
    @staticmethod
    def _empty_identifier():
        return Identifier([Token("", "")])

    @staticmethod
    def _not_empty_identifier(token):
        return token.value != ""

    @staticmethod
    def _rebuild_stmt(tokens):
        # Check if last is semicolon
        flat_token_list = []
        for token in tokens:
            flat_tokens = list(token.flatten())
            if flat_tokens[-1].match(sqlparse.tokens.Punctuation, ";"):
                flat_tokens.pop()
            flat_token_list.extend(flat_tokens)
        flat_token_list = SQLParser._strip_whitespace(flat_token_list)
        flat_token_list = list(filter(SQLParser._not_empty_identifier, flat_token_list))
        stmt = " ".join([token.value for token in flat_token_list]) + ";"
        return stmt


    @staticmethod
    def _strip_whitespace(tokens):
        return list(filter(lambda token: not token.match(sqlparse.tokens.Whitespace, " "), tokens))