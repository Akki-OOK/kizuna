#include "sql/dml_parser.h"

#include <cctype>
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
            std::string_view input_;
            std::vector<Token> tokens_;

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
                        size_t start = pos++;
                        std::string literal;
                        bool closed = false;
                        while (pos < size)
                        {
                            char curr = data[pos++];
                            if (curr == '\'')
                            {
                                if (pos < size && data[pos] == '\'')
                                {
                                    literal.push_back('\'');
                                    ++pos;
                                }
                                else
                                {
                                    closed = true;
                                    break;
                                }
                            }
                            else
                            {
                                literal.push_back(curr);
                            }
                        }
                        if (!closed)
                        {
                            Token tok;
                            tok.type = TokenType::STRING;
                            tok.text = literal;
                            tok.upper = literal;
                            tok.position = start;
                            tokens_.push_back(std::move(tok));
                            break;
                        }
                        Token tok;
                        tok.type = TokenType::STRING;
                        tok.text = literal;
                        tok.upper = literal;
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    Token tok;
                    tok.type = TokenType::SYMBOL;
                    tok.symbol = ch;
                    tok.text.assign(1, ch);
                    tok.upper = tok.text;
                    tok.position = pos;
                    tokens_.push_back(tok);
                    ++pos;
                }
                tokens_.push_back(Token{TokenType::END, "", "", 0, input_.size()});
            }
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
                if (!match_keyword("INTO"))
                {
                    syntax_error(peek(), "INTO");
                }
                std::string table = expect_identifier("table name");
                InsertStatement stmt;
                stmt.table_name = std::move(table);
                if (match_symbol('('))
                {
                    if (match_symbol(')'))
                    {
                        syntax_error(prev(), "column list");
                    }
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
                if (!match_symbol('*'))
                {
                    syntax_error(peek(), "*");
                }
                expect_keyword("FROM");
                SelectStatement stmt;
                stmt.table_name = expect_identifier("table name");
                consume_semicolon();
                expect_end();
                return stmt;
            }

            DeleteStatement parse_delete()
            {
                expect_keyword("DELETE");
                if (!match_keyword("FROM"))
                {
                    syntax_error(peek(), "FROM");
                }
                DeleteStatement stmt;
                stmt.table_name = expect_identifier("table name");
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

            const Token &consume()
            {
                return tokens_[position_++];
            }

            bool match_symbol(char symbol)
            {
                if (peek().type == TokenType::SYMBOL && peek().symbol == symbol)
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
                    syntax_error(tok, "literal");
                }
                if (tok.type == TokenType::SYMBOL && tok.symbol == '(')
                {
                    syntax_error(tok, "literal");
                }
                syntax_error(tok, "literal");
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
        if (first.upper == "TRUNCATE")
        {
            result.kind = DMLStatementKind::TRUNCATE;
            result.truncate = parser.parse_truncate();
            return result;
        }
        throw QueryException::syntax_error(sql, first.position, "DML statement");
    }
}

