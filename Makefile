include /home/system/Development/Switch/Makefile.dfl
CXXFLAGS:=$(CPPFLAGS_SANITY_DEBUG) -fsanitize=address
LDFLAGS:=$(LDFLAGS_SANITY) -L$(SWITCH_BASE) -lswitch -lpthread -ldl -lcrypto -lz -lssl -fsanitize=address

all: app service

client: client.o
	ar rcs libtank.a client.o

service: service.o
	$(CC) service.o -o ./tank $(LDFLAGS)

app : client app.o
	$(CC) app.o  -o ./app $(LDFLAGS) -L./ -ltank -lswitch


.o: .cpp

clean:
	rm -f *.o *.a

.PHONY: clean
