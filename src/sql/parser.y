%{
#include "sql/parser.hh"
#include "sql/scanner.h"

#define yylex driver.scanner_->yylex

char* dup(const char* str) {
  size_t len = strlen(str);
  char* newstr = new char[len+1];
  strncpy(newstr, str, len+1);
  return newstr;
}
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
%token SELECT SEARCH FROM WHERE BY ORDER HAVING ASC DESC LIMIT
%token AND OR NOT NE LE GE IN BETWEEN SHOW TABLES WORKERS
%token COPY WITH FORMAT TSV
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
%type <stmt> statement select_statement show_statement copy_statement
%type <sval> table_name column_name string_literal filter_literal num_literal limit_opt copy_format show_what
%type <jsonval> select_cols select_col filter_opt filter comp_filter relop_filter filter_literals
%type <jsonval> having_opt orderby_opt orderby_cols orderby_col copy_opt copy_opts copy_opts_opt copy_source
%type <jsonval> copy_cols_opt copy_cols
%type <bval> order_opt

/* Destructors */
%destructor { delete[] $$; } IDENTIFIER STRING <sval>
%destructor { delete $$; } <jsonval> <stmt>

/* Precedence and associativity */
%left     OR
%left     AND
%right    NOT
%nonassoc '=' NE
%nonassoc '<' '>' LE GE IN
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
         | show_statement
         | copy_statement
;

select_statement: SELECT select_cols FROM table_name filter_opt having_opt orderby_opt limit_opt {
                    $$ = new Statement(Statement::Type::QUERY);
                    auto& d = $$->descriptor_;
                    d["type"] = "aggregate";
                    d["header"] = driver.add_header();
                    d["table"] = $4; delete[] $4;
                    d["select"] = *$2; delete $2;
                    if ($5 != nullptr) { d["filter"] = *$5; delete $5; }
                    if ($6 != nullptr) { d["having"] = *$6; delete $6; }
                    if ($7 != nullptr) { d["sort"] = *$7; delete $7; }
                    if ($8 != nullptr) { d["limit"] = atoi($8); delete[] $8; }
                  }
                | SELECT SEARCH '(' column_name ',' string_literal ')' FROM table_name filter_opt limit_opt {
                    $$ = new Statement(Statement::Type::QUERY);
                    auto& d = $$->descriptor_;
                    d["type"] = "search";
                    d["header"] = driver.add_header();
                    d["dimension"] = $4; delete[] $4;
                    d["term"] = $6; delete[] $6;
                    d["table"] = $9; delete[] $9;
                    if ($10 != nullptr) { d["filter"] = *$10; delete $10; }
                    if ($11 != nullptr) { d["limit"] = atoi($11); delete[] $11; }
                  }
;

show_statement: SHOW show_what {
                  $$ = new Statement(Statement::Type::QUERY);
                  auto& d = $$->descriptor_;
                  d["type"] = "show";
                  d["header"] = driver.add_header();
                  d["what"] = $2;
                  delete[] $2;
                }
;

show_what: TABLES  { $$ = dup("tables"); }
         | WORKERS { $$ = dup("workers"); }
;

copy_statement: COPY table_name copy_cols_opt FROM copy_source copy_opts_opt {
                  $$ = new Statement(Statement::Type::LOAD);
                  auto& d = $$->descriptor_;
                  d["table"] = $2; delete[] $2;
                  if ($3 != nullptr) {
                    d["columns"] = *$3; delete $3;
                  }
                  for (auto it = $5->begin(); it != $5->end(); ++it) {
                    d[it.key()] = it.value();
                  }
                  delete $5;
                  for (auto it = $6->begin(); it != $6->end(); ++it) {
                    d[it.key()] = it.value();
                  }
                  delete $6;
                }
;

copy_cols_opt: '(' copy_cols ')' { $$ = $2; }
           | /* empty */ { $$ = nullptr; }
;

copy_cols: column_name { $$ = new json(); $$->push_back($1); delete[] $1; }
         | copy_cols ',' column_name { $1->push_back($3); delete[] $3; $$ = $1; }
;

copy_source: string_literal {
               $$ = new json {{"type", "file"}, {"file", $1}}; delete[] $1;
             }
;

copy_opts_opt: WITH copy_opts { $$ = $2; }
               | copy_opts
               | /* empty */ { $$ = new json {{"format", "tsv"}}; }
;

copy_opts: copy_opt
         | copy_opts copy_opt {
             for (auto it = $2->begin(); it != $2->end(); ++it) {
               (*$1)[it.key()] = it.value();
             }
             delete $2;
             $$ = $1;
           }
;

copy_opt: FORMAT copy_format { $$ = new json {{"format", $2}}; delete[] $2; }
;

copy_format: TSV { $$ = dup("tsv"); }
;

filter_opt: WHERE filter { $$ = $2; }
          | /* empty */ { $$ = nullptr; }
;

filter: '(' filter ')' { $$ = $2; }
      | comp_filter
      | relop_filter
;
         
comp_filter: filter AND filter { $$ = new json {{"op", "and"}, {"filters", {*$1, *$3}}}; delete $1; delete $3; }
           | filter OR filter  { $$ = new json {{"op", "or"}, {"filters", {*$1, *$3}}}; delete $1; delete $3; }
           | NOT filter  { $$ = new json {{"op", "not"}, {"filter", *$2}}; delete $2; }
;

relop_filter: column_name '=' filter_literal { $$ = new json {{"op", "eq"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name '<' filter_literal { $$ = new json {{"op", "lt"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name '>' filter_literal { $$ = new json {{"op", "gt"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name LE filter_literal { $$ = new json {{"op", "le"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name GE filter_literal { $$ = new json {{"op", "ge"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name NE filter_literal { $$ = new json {{"op", "ne"}, {"column", $1}, {"value", $3}}; delete[] $1; delete[] $3; }
            | column_name IN '(' filter_literals ')' {
                $$ = new json {{"op", "in"}, {"column", $1}, {"values", *$4}}; delete[] $1; delete $4;
              }
            | column_name BETWEEN filter_literal AND filter_literal {
                $$ = new json {{"op", "and"}, {"filters", {
                  {{"op", "ge"}, {"column", $1}, {"value", $3}},
                  {{"op", "le"}, {"column", $1}, {"value", $5}}
                }}};
                delete[] $1; delete[] $3; delete[] $5;
              }
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

limit_opt: LIMIT INTVAL { $$ = $2; }
         | /* empty */ { $$ = nullptr; }

select_cols: select_col { $$ = new json(); $$->push_back(*$1); delete $1; }
           | select_cols ',' select_col { $1->push_back(*$3); delete $3; $$ = $1; }
;

select_col: column_name { $$ = new json {{"column", $1}}; delete[] $1; }
          | '*' { $$ = new json {{"column", "*"}}; }
;

filter_literals: filter_literal { $$ = new json(); $$->push_back($1); delete[] $1; }
           | filter_literals ',' filter_literal { $1->push_back($3); delete[] $3; $$ = $1; }
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
