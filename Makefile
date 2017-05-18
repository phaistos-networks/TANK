HOST:=$(shell hostname)

ifeq ($(HOST), origin)
# When building on our dev.system
	include /home/system/Development/Switch/Makefile.dfl
	CXXFLAGS:=$(CPPFLAGS_SANITY_DEBUG) #-fsanitize=address
	LDFLAGS:=$(LDFLAGS_SANITY) -L$(SWITCH_BASE) -lswitch -lpthread -ldl -lcrypto -lz -lssl -ljemalloc #-fsanitize=address
	SWITCH_LIB:=-lswitch
	#CXX:=scan-build clang++
	#CXX:=clang++
else
# Lean switch bundled in this repo
	CXXFLAGS:=-std=c++1z  -Wstrict-aliasing=2 -Wsequence-point -Warray-bounds -Wextra -Winit-self -Wformat=2 -Wno-format-nonliteral -Wformat-security \
		-Wunused-variable -Wunused-value -Wreturn-type -Wparentheses -Wmissing-braces -Wno-invalid-source-encoding -Wno-invalid-offsetof \
		-Wno-unknown-pragmas -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare -Wno-invalid-offsetof   \
		-fno-rtti -std=c++14 -ffast-math  -D_REENTRANT -DREENTRANT  -g3 -ggdb -fno-omit-frame-pointer   \
		-fno-strict-aliasing    -DLEAN_SWITCH  -ISwitch/ -Wno-uninitialized -Wno-unused-function -Wno-uninitialized -funroll-loops  -O3
	LDFLAGS:=-ldl -ffunction-sections -lpthread -ldl -lz -LSwitch/ext_snappy/ -lsnappy
	SWITCH_LIB:=
	SWITCH_DEP:=switch
	# Docker complains about clang++ dep.
	#CXX:=clang++
endif

all: service cli-tool

switch:
	make -C Switch/ext_snappy/

client: client.o $(SWITCH_DEP)
	ar rcs libtank.a client.o

service: service.o $(SWITCH_DEP)
	$(CXX) service.o -o ./tank $(LDFLAGS)

cli-tool: cli.o client $(SWITCH_DEP)
	$(CXX) cli.o -o ./tank-cli -L./ -ltank $(LDFLAGS) $(SWITCH_LIB)

.o: .cpp

clean:
	rm -f *.o *.a Switch/ext_snappy/*o Switch/ext_snappy/*.a

.PHONY: clean
