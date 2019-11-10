CXX=clang++
CXXFLAGS:=-std=c++1z  -Wstrict-aliasing=2 -Wsequence-point -Warray-bounds -Wextra -Winit-self -Wformat=2 -Wno-format-nonliteral -Wformat-security \
	  -Wno-c++1z-extensions \
	  -Wunused-variable -Wunused-value -Wreturn-type -Wparentheses -Wmissing-braces -Wno-invalid-source-encoding -Wno-invalid-offsetof \
	  -Wno-unknown-pragmas -Wno-undefined-inline -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare -Wno-invalid-offsetof   \
	  -fno-rtti -ffast-math  -D_REENTRANT -DREENTRANT  -g3 -ggdb -fno-omit-frame-pointer   \
	  -fno-strict-aliasing    -DLEAN_SWITCH  -ISwitch/ -I./ -Wno-uninitialized -Wno-unused-function -Wno-uninitialized -funroll-loops  -Ofast
OBJS:=base64.o  crypto.o   text.o

all: $(OBJS)
	@rm -f libswitch.a
	ar rcs libswitch.a $(OBJS)

clean:
	rm -f *.o

.PHONY: clean
