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
        self.parse(sql)
    
    def parse(self, sql):
        sql = sqlparse.format(sql, strip_comments=True)
        self.stmts = list(sqlparse.parse(sql))
        self._parse_stmts()
    
    def _parse_stmts(self):
        for stmt in self.stmts:
            tokens = self._strip_unwanted_tokens(stmt.tokens)
            parsed_tokens = self._handle_keywords(tokens)
            if tokens == parsed_tokens:
                self.is_modified.append(False)
            else:
                self.is_modified.append(True)
            try:
                # SQL statements
                self.operations.append(self._get_operation_type(parsed_tokens))
                self.parsed.append(self._rebuild_stmt(parsed_tokens))
            except:
                # For VersionData
                self.operations.append(parsed_tokens["operation"])
                self.parsed.append(parsed_tokens)

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
        version_indices = []
        for idx, token in enumerate(tokens):
            if token.match(sqlparse.tokens.Keyword, "VERSION"):
                version_indices.append(idx)

        SQLParser._check_version_syntax(tokens, version_indices)
        
        if version_indices:
            tokens = SQLParser._handle_version(tokens, version_indices)
        
        vtable_indices = []
        for idx, token in enumerate(tokens):
            if token.match(sqlparse.tokens.Keyword, "VTABLE"):
                vtable_indices.append(idx)

        SQLParser._check_vtable_syntax(tokens, vtable_indices)

        operation_type = SQLParser._get_operation_type(tokens)
        if vtable_indices:
            if operation_type == "select":
                tokens = SQLParser._handle_vtable(tokens, vtable_indices)
            else:
                tokens = SQLParser._parse_for_versiondata(tokens)

        return tokens
                
    @staticmethod
    def _check_version_syntax(tokens, version_indices):
        if not version_indices:
            return
        # Only "VTABLE one_table_name OF VERSION version" is valid
        op = tokens[0].value.lower()
        for version_idx in version_indices:
            if not tokens[version_idx - 3].match(sqlparse.tokens.Keyword, "VTABLE"):
                raise SyntaxError("The valid syntax is `VTABLE one_table_name OF VERSION version`")
            if not tokens[version_idx - 1].match(sqlparse.tokens.Keyword, "OF"):
                raise SyntaxError("The valid syntax is `VTABLE one_table_name OF VERSION version`")
            if not tokens[version_idx + 1].ttype is sqlparse.tokens.Literal.Number.Integer:
                raise SyntaxError("Require a version number after `VERSION`")
            if not tokens[0].match(sqlparse.tokens.Keyword.DML, "SELECT"):
                raise SyntaxError("Does not support changing data in a specific version.")

    @staticmethod
    def _check_vtable_syntax(tokens, vtable_indices):
        op = tokens[0].value.lower()
        if not vtable_indices or op == "select":
            return

        if len(vtable_indices) > 1:
            raise SyntaxError("Only support one VTABLE in an INSERT, DELETE, or UPDATE statement")
        try:
            if op == "insert":
                if not tokens[1].match(sqlparse.tokens.Keyword, "INTO"):
                    raise SyntaxError("`INSERT` requires `INTO` keyword.")
                if not tokens[2].match(sqlparse.tokens.Keyword, "VTABLE") or type(tokens[4]) is not sqlparse.sql.Values:
                    raise SyntaxError("The valid syntax is `INSERT INTO VTABLE table_name VALUES values`")
                if type(tokens[3]) is IdentifierList:
                    raise SyntaxError("`INSERT` support only one table manipulation.")
            elif op == "delete":
                if not tokens[1].match(sqlparse.tokens.Keyword, "FROM"):
                    raise SyntaxError("`DELETE` requires `FROM` keyword.")
                if not tokens[2].match(sqlparse.tokens.Keyword, "VTABLE"):
                    raise SyntaxError("The valid syntax is `DELETE FROM VTABLE table_name [WHERE conditions]`")
                if type(tokens[3]) is IdentifierList:
                    raise SyntaxError("`DELETE` support only one table manipulation.")
            elif op == "update":
                if not tokens[1].match(sqlparse.tokens.Keyword, "VTABLE"):
                    raise SyntaxError("The valid syntax is `UPDATE VTABLE table_name SET column1 = value1, ... [WHERE conditions]`")
                if not tokens[3].match(sqlparse.tokens.Keyword, "SET"):
                    raise SyntaxError("`UPDATE` requires `SET` keyword.")
                if type(tokens[2]) is IdentifierList:
                    raise SyntaxError("`UPDATE` support only one table manipulation.")
            else:
                raise SyntaxError("Only support `SELECT`, `INSERT`, `DELETE`, and `UPDATE` statements.")
        except IndexError:
            raise SyntaxError("Invalid SQL statement")

    @staticmethod
    def _handle_version(tokens, indices):
        for idx in indices:
            vtable_idx = idx - 3
            table_name_idx = idx - 2
            of_idx = idx - 1
            version_num_idx= idx + 1
            replace = (
                f"{tokens[table_name_idx].value}{DATA_TABLE_SUFFIX} "
                f"WHERE rid IN ("
                f"SELECT rid FROM {tokens[table_name_idx].value}{VERSION_TABLE_SUFFIX} "
                f"WHERE version = {tokens[version_num_idx]})"
            )
            
            where_indices = SQLParser._get_where(tokens)
            for where_idx in where_indices:
                if where_idx != idx + 2:
                    continue
                # 0: WHERE, 1: WhiteSpace
                replace += " AND "
                replace += " ".join([token.value for token in tokens[where_idx].tokens[2:]])
                replace = replace.strip(" ;")
                tokens[where_idx] = SQLParser._empty_identifier()
            
            tokens[vtable_idx] = SQLParser._empty_identifier()
            tokens[table_name_idx] = SQLParser._empty_identifier()
            tokens[of_idx] = SQLParser._empty_identifier()
            tokens[idx] = SQLParser._build_identifier(replace)
            tokens[version_num_idx] = SQLParser._empty_identifier()

        return tokens

    @staticmethod
    def _handle_vtable(tokens, vtable_indices):
        for idx in vtable_indices:
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
    def _parse_for_versiondata(tokens):
        def _parse_where_for_versiondata(tokens):
            where_indices = SQLParser._get_where(tokens)
            if len(where_indices) > 1:
                raise SyntaxError("More than one `WHERE` clause exists.")
            return tokens[where_indices[0]].value if where_indices else ""

        op = tokens[0].value.lower() 
        parsed = {"table_name": None, "attributes": {}}
        parsed["operation"] = op
        if op == "insert":
            # With column name
            if type(tokens[3]) is sqlparse.sql.Function:
                parsed["table_name"], parsed["attributes"]["columns"] = SQLParser._parse_function(tokens[3])
            else:
                parsed["table_name"] = tokens[3].value
                parsed["attributes"]["columns"] = None
            parsed["attributes"]["data"] =SQLParser._parse_values(tokens[4]) 
        elif op == "delete":
            parsed["table_name"] = tokens[3].value
            parsed["attributes"]["where"] = _parse_where_for_versiondata(tokens)
        elif op == "update":
            # UPDATE VTABLE table_name SET column1 = value1, ...
            parsed["table_name"] = tokens[2].value
            parsed["attributes"]["where"] = _parse_where_for_versiondata(tokens)
            parsed["attributes"]["set"] = SQLParser._parse_comparison(tokens[4])

        return parsed

    @staticmethod
    def _parse_values(values):
        data = []
        for token in values.tokens:
            if type(token) is sqlparse.sql.Parenthesis:
                data.append(SQLParser._parse_value_tuple(token.value))
        return data
    
    @staticmethod
    def _parse_function(function):
        if type(function.tokens[0]) is not sqlparse.sql.Identifier:
            raise SyntaxError("The valid syntax is `table_name (col1, col2, ...)`")
        name = function.tokens[0].value
        for token in function.tokens:
            if type(token) is sqlparse.sql.Parenthesis:
                args = SQLParser._parse_value_tuple(token.value)
                break
        return name, args 
    
    @staticmethod
    def _parse_value_tuple(string):
        values = string[1:-1].split(",")
        values = [value.strip() for value in values]
        return values
    
    @staticmethod
    def _parse_comparison(tokens):
        def _get_col_and_value(tokens):
            parsed = {}
            tokens = SQLParser._strip_unwanted_tokens(tokens)
            for idx, token in enumerate(tokens):
                if token.match(sqlparse.tokens.Operator.Comparison, "="):
                    parsed[tokens[idx - 1].value] = tokens[idx + 1].value
            return parsed

        parsed = {}
        for token in tokens:
            # Mulitple columns
            if type(token) is sqlparse.sql.Comparison:
                parsed.update(_get_col_and_value(token.tokens))
            # Single column
            elif token.match(sqlparse.tokens.Operator.Comparison, "="):
                parsed.update(_get_col_and_value(tokens))

        return parsed

    @staticmethod
    def _get_where(tokens):
        indices = []
        for idx, token in enumerate(tokens):
            if type(token) is Where:
                indices.append(idx)
        return  indices

    @staticmethod 
    def _get_token(tokens, ttype):
        indices = []
        for token in tokens:
            if token.ttype is ttype:
                indices.append(token)
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
        flat_token_list = SQLParser._strip_unwanted_tokens(flat_token_list)
        flat_token_list = list(filter(SQLParser._not_empty_identifier, flat_token_list))
        stmt = " ".join([token.value for token in flat_token_list]) + ";"
        return stmt

    @staticmethod
    def _strip_unwanted_tokens(tokens):
        # Remove whitespace, newline, and semicolon
        tokens = list(filter(lambda token: not (token.match(sqlparse.tokens.Whitespace, " ") or token.match(sqlparse.tokens.Newline, "\n")), tokens))
        tokens = tokens[:-1] if tokens[-1].match(sqlparse.tokens.Punctuation, ";") else tokens
        return tokens
