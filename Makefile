HOST:=$(shell hostname)


ifeq ($(HOST), origin)
 ORIGIN=1
else
 ifeq ($(HOST), nigiro)
  ORIGIN=1
 endif
endif



ifeq ($(ORIGIN), 1)
# When building on our dev.system
	include /home/system/Development/Switch/Makefile.dfl
	LIBTREE_PATH:=/home/system/Development/Switch/ext/ebtree/libtree.a
	CXXFLAGS:=$(CPPFLAGS_SANITY_RELEASE) #-fsanitize=address
	LDFLAGS:=$(LDFLAGS_SANITY) -L$(SWITCH_BASE) -lswitch -pthread -ldl \
	/usr/lib/x86_64-linux-gnu/libssl.a /usr/lib/x86_64-linux-gnu/libcrypto.a  \
	-lz -ljemalloc  $(LIBTREE_PATH)
	SWITCH_LIB:=-lswitch
	CXXFLAGS += -DTRACE_REACHABILITY_OVERSEER
else
# Lean switch bundled in this repo
	CXX:=clang++
	LIBTREE_PATH:=$(shell pwd)/Switch/ext/ebtree/libtree.a
	CXXFLAGS:=-stdlib=libc++ -std=c++2a -Wstrict-aliasing=2 -Wsequence-point -Warray-bounds -Wextra -Winit-self -Wformat=2 -Wno-format-nonliteral -Wformat-security \
		-Wno-c++1z-extensions \
		-I ./Switch/ext/ \
		-Wunused-variable -Wunused-value -Wreturn-type -Wparentheses -Wmissing-braces -Wno-invalid-source-encoding -Wno-invalid-offsetof  -Wno-c99-designator \
		-Wno-unknown-pragmas -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare -Wno-invalid-offsetof   \
		-fno-rtti -ffast-math  -D_REENTRANT -DREENTRANT  -g3 -ggdb -fno-omit-frame-pointer   \
		-fno-strict-aliasing    -DLEAN_SWITCH  -ISwitch/ -I./ -Wno-uninitialized -Wno-unused-function -Wno-uninitialized -funroll-loops  -Ofast
	LDFLAGS:=-stdlib=libc++ -ldl -ffunction-sections -pthread -ldl -lz -LSwitch/ext_snappy/ -lsnappy $(LIBTREE_PATH)
	SWITCH_LIB:=Switch/libswitch.a
	SWITCH_DEP:=switch
	EXT_DEP:=ext
	# Docker complains about clang++ dep.
	#CXX:=clang++
endif
CXXFLAGS +=  -Wstrict-aliasing

SERVICE_OBJS:=$(patsubst %.cpp,%.o,$(wildcard service*.cpp))
CLIENT_OBJS:=$(patsubst %.cpp,%.o,$(wildcard client*.cpp))
TEST_SERVICE_OBJS:=$(patsubst %.cpp,%.o,$(wildcard test_service*.cpp))
TEST_CLIENT_OBJS:=$(patsubst %.cpp,%.o,$(wildcard test_client*.cpp))


all: service cli-tool client

switch:
	@make -C Switch/ext_snappy/ all
	@make -C Switch/ all

ext:
	@make -C Switch/ext/ebtree/ all

test_client: $(CLIENT_OBJS) $(SWITCH_DEP) $(EXT_DEP) $(TEST_CLIENT_OBJS)
	$(CXX) $(CLIENT_OBJS) $(TEST_CLIENT_OBJS) $(LDFLAGS) $(SWITCH_LIB) -o ./test_client
	./test_client -a process_undeliverable_broker_req


# we are going to include the $(LIBTREE_PATH) members into libtank.a so that
# users won't need to also link against $(LIBTREE_PATH)
client: $(CLIENT_OBJS) $(SWITCH_DEP) $(EXT_DEP)
	@mkdir -p .objs
	@cd .objs ; ar x $(LIBTREE_PATH)
	@ar rcs libtank.a $(CLIENT_OBJS) .objs/*.o
	@rm -rf .objs
	@echo "You can now link against libtank.a"



service: $(SERVICE_OBJS) $(SWITCH_DEP) $(EXT_DEP)
	@$(CXX) $(SERVICE_OBJS) -o ./tank $(LDFLAGS)  $(SWITCH_LIB)

test_service: $(CLIENT_OBJS) $(SWITCH_DEP) $(EXT_DEP) $(TEST_SERVICE_OBJS)
	$(CXX) $(CLIENT_OBJS) $(TEST_SERVICE_OBJS)  $(shell ls service*.o | grep -v service_main.o) -o ./test_service $(LDFLAGS) $(SWITCH_LIB) 
	./test_service -a
	
cli-tool: cli.o client $(SWITCH_DEP)
	@$(CXX) cli.o -o ./tank-cli -L./ -ltank $(LDFLAGS) $(SWITCH_LIB)
	@echo "./tank-cli built. You may want to copy it to e.g /usr/bin, or /usr/local/bin or somewhere else"

$(SERVICE_OBJS): %.o: %.cpp service.h common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@ 


$(CLIENT_OBJS): %.o: %.cpp tank_client.h common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@


$(TEST_SERVICE_OBJS): %.o: %.cpp service.h common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@ 


$(TEST_CLIENT_OBJS): %.o: %.cpp tank_client.h common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@


.o: .cpp

clean:
	rm -f Switch/*.o
	rm -f Switch/ext/*.o
	rm -f Switch/ext/ebtree/*.o
	rm -f ./*.o
	rm -f ./*.a
	rm -f Switch/ext_snappy/*.o
	rm -f ext/ebtree/*.o
	rm -f tank-cli test_client test_service tank

.PHONY: clean ext switch all
