#include "gtest/gtest.h"
#include "util/config.h"
#include "codegen/compiler.h"

namespace cg = viya::codegen;
namespace util = viya::util;

TEST(Codegen, Basic)
{
  std::string code = "#include \"util/config.h\"\nint viya_foo() __attribute__((__visibility__(\"default\"))); int viya_foo() { return 123; }";
  util::Config config;
  cg::Compiler compiler(config);
  auto library = compiler.Compile(code);
  auto func = library->GetFunction<int (*)()>(std::string("_Z8viya_foov"));

  EXPECT_EQ(123, func());
}

