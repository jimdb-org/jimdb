set(parserSRCS
    src/SQLParser.cpp
    src/SQLParserResult.cpp
    src/parser/bison_parser.cpp
    src/parser/flex_lexer.cpp
    src/sql/Expr.cpp
    src/sql/PrepareStatement.cpp
    src/sql/SQLStatement.cpp
    src/sql/statements.cpp
    src/util/sqlhelper.cpp
    )

add_library(sqlparser SHARED ${parserSRCS})
