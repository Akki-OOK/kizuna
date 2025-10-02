#include "sql/dml_parser.h"

#include <cctype>
#include <limits>
#include <optional>
#include <utility>

#include "common/exception.h"

namespace kizuna::sql
{
    namespace
    {
        enum class TokenType
        {
            IDENT,
            NUMBER,
            STRING,
            SYMBOL,
            END
        };

        struct Token
        {
            TokenType type{TokenType::END};
            std::string text;
            std::string upper;
            char symbol{0};
            size_t position{0};
        };

        class Lexer
        {
        public:
            explicit Lexer(std::string_view input)
                : input_(input)
            {
                tokenize();
            }

            const std::vector<Token> &tokens() const { return tokens_; }

        private:
            static bool is_identifier_start(char ch)
            {
                return std::isalpha(static_cast<unsigned char>(ch)) || ch == '_';
            }

            static bool is_identifier_part(char ch)
            {
                return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
            }

            static std::string to_upper(std::string_view text)
            {
                std::string upper(text);
                for (char &c : upper)
                    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
                return upper;
            }

            void tokenize()
            {
                const char *data = input_.data();
                const size_t size = input_.size();
                size_t pos = 0;
                while (pos < size)
                {
                    const char ch = data[pos];
                    if (std::isspace(static_cast<unsigned char>(ch)))
                    {
                        ++pos;
                        continue;
                    }

                    if (is_identifier_start(ch))
                    {
                        const size_t start = pos++;
                        while (pos < size && is_identifier_part(data[pos]))
                            ++pos;
                        std::string word(data + start, pos - start);
                        Token tok;
                        tok.type = TokenType::IDENT;
                        tok.text = word;
                        tok.upper = to_upper(word);
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    if ((ch == '-' && pos + 1 < size && std::isdigit(static_cast<unsigned char>(data[pos + 1]))) ||
                        std::isdigit(static_cast<unsigned char>(ch)))
                    {
                        const size_t start = pos;
                        bool seen_dot = false;
                        if (ch == '-')
                            ++pos;
                        while (pos < size)
                        {
                            const char cur = data[pos];
                            if (std::isdigit(static_cast<unsigned char>(cur)))
                            {
                                ++pos;
                                continue;
                            }
                            if (cur == '.' && !seen_dot)
                            {
                                seen_dot = true;
                                ++pos;
                                continue;
                            }
                            break;
                        }
                        std::string number(data + start, pos - start);
                        Token tok;
                        tok.type = TokenType::NUMBER;
                        tok.text = number;
                        tok.upper = number;
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    if (ch == '\'')
                    {
                        const size_t start = pos++;
                        std::string literal;
                        literal.reserve(16);
                        bool closed = false;
                        while (pos < size)
                        {
                            const char cur = data[pos++];
                            if (cur == '\'' && pos < size && data[pos] == '\'')
                            {
                                literal.push_back('\'');
                                ++pos;
                            }
                            else if (cur == '\'')
                            {
                                closed = true;
                                break;
                            }
                            else
                            {
                                literal.push_back(cur);
                            }
                        }
                        if (!closed)
                        {
                            Token tok;
                            tok.type = TokenType::END;
                            tok.position = start;
                            tokens_.clear();
                            tokens_.push_back(tok);
                            return;
                        }
                        Token tok;
                        tok.type = TokenType::STRING;
                        tok.text = literal;
                        tok.upper = literal;
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    // multi-character comparison operators
                    if (ch == '!' || ch == '<' || ch == '>' || ch == '=')
                    {
                        const size_t start = pos;
                        std::string text(1, ch);
                        if (pos + 1 < size)
                        {
                            const char next = data[pos + 1];
                            if ((ch == '!' && next == '=') ||
                                (ch == '<' && (next == '=' || next == '>')) ||
                                (ch == '>' && next == '='))
                            {
                                text.push_back(next);
                                ++pos;
                            }
                        }
                        Token tok;
                        tok.type = TokenType::SYMBOL;
                        tok.symbol = text[0];
                        tok.text = text;
                        tok.upper = text;
                        tok.position = start;
                        ++pos;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    // Single-character symbols
                    Token tok;
                    tok.type = TokenType::SYMBOL;
                    tok.symbol = ch;
                    tok.text = std::string(1, ch);
                    tok.upper = tok.text;
                    tok.position = pos;
                    ++pos;
                    tokens_.push_back(std::move(tok));
                }

                Token end;
                end.type = TokenType::END;
                end.position = input_.size();
                tokens_.push_back(std::move(end));
            }

            std::string_view input_;
            std::vector<Token> tokens_;
        };

        class Parser
        {
        public:
            Parser(std::string_view input, const std::vector<Token> &tokens)
                : input_(input), tokens_(tokens)
            {
            }

            InsertStatement parse_insert()
            {
                expect_keyword("INSERT");
                expect_keyword("INTO");
                InsertStatement stmt;
                stmt.table_name = expect_identifier("table name");
                if (match_symbol('('))
                {
                    if (match_symbol(')'))
                        syntax_error(prev(), "column list");
                    do
                    {
                        stmt.column_names.push_back(expect_identifier("column name"));
                    } while (match_symbol(','));
                    expect_symbol(')');
                }
                expect_keyword("VALUES");
                do
                {
                    stmt.rows.push_back(parse_row());
                } while (match_symbol(','));
                consume_semicolon();
                expect_end();
                return stmt;
            }

            SelectStatement parse_select()
            {
                expect_keyword("SELECT");
                SelectStatement stmt;
                stmt.columns = parse_select_list();
                expect_keyword("FROM");
                stmt.table_name = expect_identifier("table name");
                if (match_keyword("WHERE"))
                {
                    stmt.where = parse_expression();
                }
                if (match_keyword("LIMIT"))
                {
                    stmt.limit = parse_limit_value();
                }
                consume_semicolon();
                expect_end();
                return stmt;
            }

            DeleteStatement parse_delete()
            {
                expect_keyword("DELETE");
                expect_keyword("FROM");
                DeleteStatement stmt;
                stmt.table_name = expect_identifier("table name");
                if (match_keyword("WHERE"))
                {
                    stmt.where = parse_expression();
                }
                consume_semicolon();
                expect_end();
                return stmt;
            }

            UpdateStatement parse_update()
            {
                expect_keyword("UPDATE");
                UpdateStatement stmt;
                stmt.table_name = expect_identifier("table name");
                expect_keyword("SET");
                do
                {
                    std::string column = expect_identifier("column name");
                    expect_symbol('=');
                    auto value = parse_expression();
                    UpdateAssignment assignment;
                    assignment.column_name = std::move(column);
                    assignment.value = std::move(value);
                    stmt.assignments.push_back(std::move(assignment));
                } while (match_symbol(','));

                if (stmt.assignments.empty())
                {
                    syntax_error(peek(), "assignment");
                }

                if (match_keyword("WHERE"))
                {
                    stmt.where = parse_expression();
                }
                consume_semicolon();
                expect_end();
                return stmt;
            }

            TruncateStatement parse_truncate()
            {
                expect_keyword("TRUNCATE");
                match_keyword("TABLE");
                TruncateStatement stmt;
                stmt.table_name = expect_identifier("table name");
                consume_semicolon();
                expect_end();
                return stmt;
            }

            const Token &peek(size_t offset = 0) const
            {
                size_t index = position_ + offset;
                if (index >= tokens_.size())
                    return tokens_.back();
                return tokens_[index];
            }

            const Token &prev() const
            {
                if (position_ == 0) return tokens_.front();
                return tokens_[position_ - 1];
            }

            bool match_symbol(char symbol)
            {
                if (peek().type == TokenType::SYMBOL && peek().text.size() == 1 && peek().symbol == symbol)
                {
                    ++position_;
                    return true;
                }
                return false;
            }

            bool match_symbol_text(std::string_view symbol)
            {
                if (peek().type == TokenType::SYMBOL && peek().text == symbol)
                {
                    ++position_;
                    return true;
                }
                return false;
            }

            void expect_symbol(char symbol)
            {
                if (!match_symbol(symbol))
                {
                    syntax_error(peek(), std::string("'") + symbol + "'");
                }
            }

            bool match_keyword(std::string_view keyword)
            {
                if (peek().type == TokenType::IDENT && peek().upper == keyword)
                {
                    ++position_;
                    return true;
                }
                return false;
            }

            void expect_keyword(std::string_view keyword)
            {
                if (!match_keyword(keyword))
                {
                    syntax_error(peek(), keyword);
                }
            }

            std::string expect_identifier(std::string_view what)
            {
                const Token &tok = peek();
                if (tok.type != TokenType::IDENT)
                {
                    syntax_error(tok, what);
                }
                ++position_;
                return tok.text;
            }

            void consume_semicolon()
            {
                match_symbol(';');
            }

            void expect_end()
            {
                if (peek().type != TokenType::END)
                {
                    syntax_error(peek(), "end of statement");
                }
            }

        private:
            InsertRow parse_row()
            {
                expect_symbol('(');
                InsertRow row;
                if (match_symbol(')'))
                {
                    syntax_error(prev(), "value");
                }
                do
                {
                    row.values.push_back(parse_literal());
                } while (match_symbol(','));
                expect_symbol(')');
                return row;
            }

            std::vector<SelectItem> parse_select_list()
            {
                std::vector<SelectItem> items;
                if (match_symbol('*'))
                {
                    items.push_back(SelectItem::star());
                    return items;
                }
                do
                {
                    items.push_back(SelectItem::column_item(parse_column_ref()));
                } while (match_symbol(','));
                return items;
            }

            ColumnRef parse_column_ref()
            {
                ColumnRef ref;
                std::string first = expect_identifier("column");
                if (match_symbol('.'))
                {
                    ref.table = std::move(first);
                    ref.column = expect_identifier("column");
                }
                else
                {
                    ref.column = std::move(first);
                }
                return ref;
            }

            std::unique_ptr<Expression> parse_expression()
            {
                return parse_or();
            }

            std::unique_ptr<Expression> parse_or()
            {
                auto expr = parse_and();
                while (match_keyword("OR"))
                {
                    auto rhs = parse_and();
                    expr = Expression::make_binary(BinaryOperator::OR, std::move(expr), std::move(rhs));
                }
                return expr;
            }

            std::unique_ptr<Expression> parse_and()
            {
                auto expr = parse_not();
                while (match_keyword("AND"))
                {
                    auto rhs = parse_not();
                    expr = Expression::make_binary(BinaryOperator::AND, std::move(expr), std::move(rhs));
                }
                return expr;
            }

            std::unique_ptr<Expression> parse_not()
            {
                if (match_keyword("NOT"))
                {
                    auto operand = parse_not();
                    return Expression::make_unary(UnaryOperator::NOT, std::move(operand));
                }
                return parse_comparison();
            }

            std::unique_ptr<Expression> parse_comparison()
            {
                auto left = parse_primary();
                if (match_symbol_text("="))
                {
                    auto right = parse_primary();
                    return Expression::make_binary(BinaryOperator::EQUAL, std::move(left), std::move(right));
                }
                if (match_symbol_text("!=") || match_symbol_text("<>"))
                {
                    auto right = parse_primary();
                    return Expression::make_binary(BinaryOperator::NOT_EQUAL, std::move(left), std::move(right));
                }
                if (match_symbol_text("<="))
                {
                    auto right = parse_primary();
                    return Expression::make_binary(BinaryOperator::LESS_EQUAL, std::move(left), std::move(right));
                }
                if (match_symbol_text(">="))
                {
                    auto right = parse_primary();
                    return Expression::make_binary(BinaryOperator::GREATER_EQUAL, std::move(left), std::move(right));
                }
                if (match_symbol_text("<"))
                {
                    auto right = parse_primary();
                    return Expression::make_binary(BinaryOperator::LESS, std::move(left), std::move(right));
                }
                if (match_symbol_text(">"))
                {
                    auto right = parse_primary();
                    return Expression::make_binary(BinaryOperator::GREATER, std::move(left), std::move(right));
                }
                return left;
            }

            std::unique_ptr<Expression> parse_primary()
            {
                if (match_symbol('('))
                {
                    auto expr = parse_expression();
                    expect_symbol(')');
                    return expr;
                }

                const Token &tok = peek();
                if (is_literal_token(tok))
                {
                    auto literal = parse_literal();
                    auto expr = Expression::make_literal(std::move(literal));
                    return parse_null_test(std::move(expr));
                }

                if (tok.type == TokenType::IDENT)
                {
                    auto column = parse_column_ref();
                    auto expr = Expression::make_column(std::move(column));
                    return parse_null_test(std::move(expr));
                }

                syntax_error(tok, "expression");
            }

            std::unique_ptr<Expression> parse_null_test(std::unique_ptr<Expression> base)
            {
                if (match_keyword("IS"))
                {
                    bool is_not = match_keyword("NOT");
                    expect_keyword("NULL");
                    base = Expression::make_null_check(std::move(base), is_not);
                }
                return base;
            }

            bool is_literal_token(const Token &tok) const
            {
                if (tok.type == TokenType::STRING || tok.type == TokenType::NUMBER)
                    return true;
                if (tok.type == TokenType::IDENT)
                {
                    return tok.upper == "NULL" || tok.upper == "TRUE" || tok.upper == "FALSE";
                }
                return false;
            }

            LiteralValue parse_literal()
            {
                const Token &tok = peek();
                if (tok.type == TokenType::STRING)
                {
                    ++position_;
                    return LiteralValue::string(tok.text);
                }
                if (tok.type == TokenType::NUMBER)
                {
                    ++position_;
                    if (tok.text.find('.') != std::string::npos)
                        return LiteralValue::floating(tok.text);
                    return LiteralValue::integer(tok.text);
                }
                if (tok.type == TokenType::IDENT)
                {
                    std::string upper = tok.upper;
                    ++position_;
                    if (upper == "NULL")
                        return LiteralValue::null();
                    if (upper == "TRUE")
                        return LiteralValue::boolean(true);
                    if (upper == "FALSE")
                        return LiteralValue::boolean(false);
                }
                syntax_error(tok, "literal");
            }

            std::int64_t parse_limit_value()
            {
                const Token &tok = peek();
                if (tok.type != TokenType::NUMBER || tok.text.find('.') != std::string::npos)
                {
                    syntax_error(tok, "integer literal");
                }
                ++position_;
                try
                {
                    long long value = std::stoll(tok.text);
                    if (value < 0)
                        throw std::out_of_range("negative");
                    return static_cast<std::int64_t>(value);
                }
                catch (...)
                {
                    syntax_error(tok, "non-negative integer");
                }
            }

            [[noreturn]] void syntax_error(const Token &tok, std::string_view expected)
            {
                throw QueryException::syntax_error(input_, tok.position, expected);
            }

            std::string_view input_;
            const std::vector<Token> &tokens_;
            size_t position_{0};
        };
    } // namespace

    InsertStatement parse_insert(std::string_view sql)
    {
        Lexer lexer(sql);
        Parser parser(sql, lexer.tokens());
        return parser.parse_insert();
    }

    SelectStatement parse_select(std::string_view sql)
    {
        Lexer lexer(sql);
        Parser parser(sql, lexer.tokens());
        return parser.parse_select();
    }

    DeleteStatement parse_delete(std::string_view sql)
    {
        Lexer lexer(sql);
        Parser parser(sql, lexer.tokens());
        return parser.parse_delete();
    }

    UpdateStatement parse_update(std::string_view sql)
    {
        Lexer lexer(sql);
        Parser parser(sql, lexer.tokens());
        return parser.parse_update();
    }

    TruncateStatement parse_truncate(std::string_view sql)
    {
        Lexer lexer(sql);
        Parser parser(sql, lexer.tokens());
        return parser.parse_truncate();
    }

    ParsedDML parse_dml(std::string_view sql)
    {
        Lexer lexer(sql);
        Parser parser(sql, lexer.tokens());
        const Token &first = parser.peek();
        if (first.type != TokenType::IDENT)
        {
            throw QueryException::syntax_error(sql, first.position, "statement");
        }
        ParsedDML result;
        if (first.upper == "INSERT")
        {
            result.kind = DMLStatementKind::INSERT;
            result.insert = parser.parse_insert();
            return result;
        }
        if (first.upper == "SELECT")
        {
            result.kind = DMLStatementKind::SELECT;
            result.select = parser.parse_select();
            return result;
        }
        if (first.upper == "DELETE")
        {
            result.kind = DMLStatementKind::DELETE;
            result.del = parser.parse_delete();
            return result;
        }
        if (first.upper == "UPDATE")
        {
            result.kind = DMLStatementKind::UPDATE;
            result.update = parser.parse_update();
            return result;
        }
        if (first.upper == "TRUNCATE")
        {
            result.kind = DMLStatementKind::TRUNCATE;
            result.truncate = parser.parse_truncate();
            return result;
        }
        throw QueryException::syntax_error(sql, first.position, "DML statement");
    }
}

