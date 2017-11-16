%{
#include "sql/parser.hh"
#include "sql/scanner.h"
#include "sql/driver.h"

/* Defines some macros to update locations */

#define STEP()     do { driver.location_->step(); } while(0)
#define COL(Col)   driver.location_->columns(Col)
#define LINE(Line) do { driver.location_->lines(Line); } while(0)
#define DUP()      do { yylval->sval = new char[yyleng+1]; \
  strncpy(yylval->sval, yytext, yyleng); yylval->sval[yyleng] = '\0'; } while(0)
#define DUP_NQ()   do { yylval->sval = new char[yyleng-1]; \
  strncpy(yylval->sval, yytext+1, yyleng-2); yylval->sval[yyleng-2] = '\0'; } while(0)
#define YY_USER_ACTION COL(yyleng);

typedef viya::sql::Parser::token token;
typedef viya::sql::Parser::token_type token_type;

#define yyterminate() return token::TOK_EOF
%}

%option debug
%option c++
%option noyywrap
%option never-interactive
%option case-insensitive
%option yylineno
%option nounput
%option batch
%option prefix="parse"

%s COMMENT

%%
 /* The rules */
%{
  // reset location
  STEP();
%}

[ \t]+          STEP();

[\n\r]+         LINE(yyleng);

--              BEGIN(COMMENT);
<COMMENT>[^\n]* /* skipping comment content until a end of line is read */;
<COMMENT>\n     BEGIN(INITIAL);

SELECT          { return token::SELECT; }
SEARCH          { return token::SEARCH; }
FROM            { return token::FROM; }
WHERE           { return token::WHERE; }
BY              { return token::BY; }
HAVING          { return token::HAVING; }
ORDER           { return token::ORDER; }
ASC             { return token::ASC; }
DESC            { return token::DESC; }
LIMIT           { return token::LIMIT; }
AND             { return token::AND; }
OR              { return token::OR; }
NOT             { return token::NOT; }
IN              { return token::IN; }
BETWEEN         { return token::BETWEEN; }
SHOW            { return token::SHOW; }
TABLES          { return token::TABLES; }
COPY            { return token::COPY; }
WITH            { return token::WITH; }
FORMAT          { return token::FORMAT; }
TSV             { return token::TSV; }
"!="            { return token::NE; }
"<>"            { return token::NE; }
"<="            { return token::LE; }
">="            { return token::GE; }

[-+*/(){},.;<>=^%:?[\]|] { return static_cast<token_type>(*yytext); }

-?[0-9]+"."[0-9]* |
"."[0-9]* { DUP(); return token::FLOATVAL; }

-?[0-9]+  { DUP(); return token::INTVAL; }

[A-Za-z][A-Za-z0-9_]* { DUP(); return token::IDENTIFIER; }

\"[^\"\n]+\" { DUP_NQ(); return token::IDENTIFIER; }

\`[^\`\n]+\` { DUP_NQ(); return token::IDENTIFIER; }

'[^'\n]*' { DUP_NQ(); return token::STRING; }

. {
  std::ostringstream err;
  err<<"Unexpected token at "<<*driver.location_<<": "<<*yytext;
  driver.AddError(err.str());
  STEP();
}
%%
/*
   CUSTOM C++ CODE
*/

namespace viya {
namespace sql {

Scanner::Scanner():parseFlexLexer() {
}

Scanner::~Scanner() {
}

}}

#ifdef yylex
# undef yylex
#endif

int parseFlexLexer::yylex() {
  return 0;
}
