#include <ext/Catch/catch.hpp>
#include "service.h"

TEST_CASE("Utilities") {
	REQUIRE(TANKUtil::minimum(100,10,50,2,70,80) == 2);
	REQUIRE(TANKUtil::minimum(10,20,30,40,50) == 10);
	REQUIRE(TANKUtil::minimum(100, 10, 50, 70, 2) == 2);
}
