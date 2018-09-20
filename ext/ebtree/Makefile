CC:=clang
CFLAGS:=-Wno-address-of-packed-member -g3 -ggdb -Wall -Ofast 
OBJS:=eb32tree.o  eb64tree.o  ebimtree.o  ebistree.o  ebmbtree.o  ebpttree.o  ebsttree.o  ebtree.o

all: lib

lib: $(OBJS)
	@rm -f libebtree.a
	ar rcs libtree.a $(OBJS)

clean:
	rm -f *.o

.PHONY: clean
