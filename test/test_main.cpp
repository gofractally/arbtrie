#define CATCH_CONFIG_RUNNER
#include <catch2/catch_session.hpp>
#include <cstdlib>

int main(int argc, char* argv[])
{
   return Catch::Session().run(argc, argv);
}
