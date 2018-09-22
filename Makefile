HOST:=$(shell hostname)

ifeq ($(HOST), origin)
# When building on our dev.system
	include /home/system/Development/Switch/Makefile.dfl
	CXXFLAGS:=$(CPPFLAGS_SANITY_RELEASE) #-fsanitize=address
	LDFLAGS:=$(LDFLAGS_SANITY) -L$(SWITCH_BASE) -lswitch -lpthread -ldl /usr/lib/x86_64-linux-gnu/libssl.a /usr/lib/x86_64-linux-gnu/libcrypto.a  -lz -ljemalloc  /home/system/Development/Switch/ext/ebtree/libtree.a
	SWITCH_LIB:=-lswitch
	CXXFLAGS += -DTRACE_REACHABILITY_OVERSEER
	#CXX:=scan-build clang++
	#CXX:=clang++
else
# Lean switch bundled in this repo
	CXX:=clang++
	CXXFLAGS:=-std=c++1z  -Wstrict-aliasing=2 -Wsequence-point -Warray-bounds -Wextra -Winit-self -Wformat=2 -Wno-format-nonliteral -Wformat-security \
		-Wno-c++1z-extensions \
		-Wunused-variable -Wunused-value -Wreturn-type -Wparentheses -Wmissing-braces -Wno-invalid-source-encoding -Wno-invalid-offsetof \
		-Wno-unknown-pragmas -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare -Wno-invalid-offsetof   \
		-fno-rtti -std=c++14 -ffast-math  -D_REENTRANT -DREENTRANT  -g3 -ggdb -fno-omit-frame-pointer   \
		-fno-strict-aliasing    -DLEAN_SWITCH  -ISwitch/ -I./ -Wno-uninitialized -Wno-unused-function -Wno-uninitialized -funroll-loops  -Ofast
	LDFLAGS:=-ldl -ffunction-sections -lpthread -ldl -lz -LSwitch/ext_snappy/ -lsnappy ext/ebtree/libtree.a
	SWITCH_LIB:=
	SWITCH_DEP:=switch
	EXT_DEP:=ext
	# Docker complains about clang++ dep.
	#CXX:=clang++
endif

all: service cli-tool client

switch:
	+make -C Switch/ext_snappy/ all

ext:
	+make -C ext/ebtree/ all

client: client.o $(SWITCH_DEP) $(EXT_DEP)
	ar rcs libtank.a client.o

service: service.o $(SWITCH_DEP) $(EXT_DEP)
	$(CXX) service.o -o ./tank $(LDFLAGS) 

cli-tool: cli.o client $(SWITCH_DEP)
	$(CXX) cli.o -o ./tank-cli -L./ -ltank $(LDFLAGS) $(SWITCH_LIB)

.o: .cpp

clean:
	rm -f ./*.o
	rm -f ./*.a
	rm -f Switch/ext_snappy/*.o
	rm -f ext/ebtree/*.o

.PHONY: clean ext switch
