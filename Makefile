include /home/system/Development/Switch/Makefile.dfl
CXXFLAGS:=$(CPPFLAGS_SANITY_DEBUG) -fsanitize=address
LDFLAGS:=$(LDFLAGS_SANITY) -L$(SWITCH_BASE) -lswitch -lpthread -ldl -lcrypto -lz -lssl -fsanitize=address

all: client service

client: client.o
	$(CC) client.o -o ./client $(LDFLAGS)

service: service.o
	$(CC) service.o -o ./service $(LDFLAGS)


.o: .cpp

clean:
	rm -f *.o *.a

.PHONY: clean
