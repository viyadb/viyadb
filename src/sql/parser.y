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
%token SELECT FROM WHERE GROUP BY ORDER HAVING ASC DESC LIMIT
%token AND OR NOT NE LE GE
%token <sval> IDENTIFIER STRING FLOATVAL INTVAL

/* Data types */
%union
{
  /* YYLTYPE */
  char* sval;
  std::vector<char*>* str_list;
  Statement* stmt;
  json* filter;
}

/* Non-terminal types */
%type <stmt> statement select_statement select_clause
%type <str_list> column_list
%type <sval> table_name column_name string_literal filter_literal num_literal
%type <filter> opt_filter filter comp_filter relop_filter

/* Destructors */
%destructor { delete[] $$; } IDENTIFIER STRING <sval>
%destructor { delete $$; } <filter>
%destructor { delete $$; } <stmt>
%destructor {
  if (($$) != nullptr) {
    for (auto ptr : *($$)) {
      delete[] ptr;
    }
    delete ($$);
  }
} <str_list>

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

select_statement: select_clause
;

select_clause: SELECT column_list FROM table_name
               opt_filter {
                 $$ = new Statement(Statement::Type::QUERY);
                 auto& d = $$->descriptor();
                 d["type"] = "aggregate";
                 d["table"] = $4;
                 json cols = json::array();
                 for (auto col : *$2) {
                   cols.push_back({{"column", col}});
                 }
                 d["select"] = cols;
                 d["filter"] = *$5;
                 for (auto c : *$2) { delete[] c; }
                 delete $2;
                 delete[] $4;
                 delete $5;
               }
;

opt_filter: WHERE filter { $$ = $2; }
          | /* empty */ { $$ = new json {{}}; }

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

column_list: column_name { $$ = new std::vector<char*>(); $$->push_back($1); }
           | column_list ',' column_name { $1->push_back($3); $$ = $1; }
;

filter_literal: string_literal | num_literal
;

string_literal: STRING
;

num_literal: FLOATVAL | INTVAL
;

table_name: IDENTIFIER
;

column_name: IDENTIFIER | '*' { $$ = new char[2] {'*', '\0'}; }
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
