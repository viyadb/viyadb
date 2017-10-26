#ifndef VIYA_SQL_SCANNER_H_
#define VIYA_SQL_SCANNER_H_

#include "sql/parser.hh"

#ifndef YY_DECL
# define YY_DECL viya::sql::Parser::token_type viya::sql::Scanner::yylex( \
    viya::sql::Parser::semantic_type* yylval, \
    viya::sql::Parser::location_type*,        \
    viya::sql::Driver& driver)
#endif

#ifndef __FLEX_LEXER_H
# define yyFlexLexer parseFlexLexer
# include <FlexLexer.h>
# undef yyFlexLexer
#endif

namespace viya {
namespace sql {

class Scanner: public parseFlexLexer {
  public:
    Scanner();
    virtual ~Scanner();

    virtual Parser::token_type yylex(
      Parser::semantic_type* yylval, Parser::location_type* l, Driver& driver);
};

}}

#endif // VIYA_SQL_SCANNER_H_
