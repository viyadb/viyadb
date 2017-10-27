%{
#include "sql/parser.hh"
#include "sql/scanner.h"

#define yylex driver.scanner_->yylex
%}

%code requires
{
  #include <sstream>
  #include "sql/driver.h"
  #include "sql/statement.h"
  #include "sql/location.hh"
  #include "sql/position.hh"
}

%code provides
{
  namespace viya { namespace sql { class Driver; }}
}

%require "2.4"
%language "C++"
%locations
%defines
%debug
%define api.namespace { viya::sql }
%define parser_class_name { Parser }
%parse-param { Driver& driver }
%lex-param { Driver& driver }
%error-verbose

/* Tokens */
%token TOK_EOF 0 "end of file"
%token SELECT FROM WHERE BY ORDER HAVING ASC DESC LIMIT
%token AND OR NOT NE LE GE
%token <sval> IDENTIFIER STRING FLOATVAL INTVAL

/* Data types */
%union
{
  /* YYLTYPE */
  char* sval;
  bool bval;
  Statement* stmt;
  json* jsonval;
}

/* Non-terminal types */
%type <stmt> statement select_statement 
%type <sval> table_name column_name string_literal filter_literal num_literal
%type <jsonval> select_cols select_col filter_opt filter comp_filter relop_filter
%type <jsonval> having_opt orderby_opt orderby_cols orderby_col
%type <bval> order_opt

/* Destructors */
%destructor { delete[] $$; } IDENTIFIER STRING <sval>
%destructor { delete $$; } <jsonval> <stmt>

/* Precedence and associativity */
%left     OR
%left     AND
%right    NOT
%nonassoc '=' NE
%nonassoc '<' '>' LE GE
%left     '[' ']'
%left     '(' ')'
%left     '.'

/* Entry point of grammar */
%start start

%%
start: statement_list
     | statement_list ';'
;

statement_list: statement { driver.AddStatement($1); }
              | statement_list ';' statement { driver.AddStatement($3); }
;

statement: select_statement
;

select_statement: SELECT select_cols FROM table_name
               filter_opt having_opt orderby_opt {
                 $$ = new Statement(Statement::Type::QUERY);
                 auto& d = $$->descriptor();
                 d["type"] = "aggregate";
                 d["table"] = $4; delete[] $4;
                 d["select"] = *$2; delete $2;
                 if ($5 != nullptr) { d["filter"] = *$5; delete $5; }
                 if ($6 != nullptr) { d["having"] = *$6; delete $6; }
                 if ($7 != nullptr) { d["sort"] = *$7; delete $7; }
               }
;

filter_opt: WHERE filter { $$ = $2; }
          | /* empty */ { $$ = nullptr; }

filter: comp_filter
      | relop_filter
;
         
comp_filter: filter AND filter { $$ = new json {{"op", "and"}, {"filters", {*$1, *$3}}}; delete $1; delete $3; }
           | filter OR filter  { $$ = new json {{"op", "or"}, {"filters", {*$1, *$3}}}; delete $1; delete $3; }
;

relop_filter: column_name '=' filter_literal { $$ = new json {{"op", "eq"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name '<' filter_literal { $$ = new json {{"op", "lt"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name '>' filter_literal { $$ = new json {{"op", "gt"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name LE filter_literal { $$ = new json {{"op", "le"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name GE filter_literal { $$ = new json {{"op", "ge"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name NE filter_literal { $$ = new json {{"op", "ne"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
;

having_opt: HAVING filter { $$ = $2; }
          | /* empty */ { $$ = nullptr; }
;

orderby_opt: ORDER BY orderby_cols { $$ = $3; }
           | /* empty */ { $$ = nullptr; }
;

orderby_cols: orderby_col { $$ = new json(); $$->push_back(*$1); delete $1; }
            | orderby_cols ',' orderby_col { $1->push_back(*$3); delete $3; $$ = $1; }
;

orderby_col: column_name order_opt { $$ = new json {{"column", $1}, {"ascending", $2}}; delete[] $1; } 
;

order_opt: ASC { $$ = true; }
         | DESC { $$ = false; }
         | /* empty */ { $$ = true; }

select_cols: select_col { $$ = new json(); $$->push_back(*$1); delete $1; }
           | select_cols ',' select_col { $1->push_back(*$3); delete $3; $$ = $1; }
;

select_col: column_name { $$ = new json {{"column", $1}}; delete[] $1; }
          | '*' { $$ = new json {{"column", "*"}}; }
;

filter_literal: string_literal | num_literal
;

string_literal: STRING
;

num_literal: FLOATVAL | INTVAL
;

table_name: IDENTIFIER
;

column_name: IDENTIFIER
;
%%

namespace viya {
namespace sql {

void Parser::error(const location&, const std::string& m) {
  std::ostringstream err;
  err<<m<<" at "<<*driver.location_;
  driver.AddError(err.str());
}

}}
