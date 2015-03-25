OPTION=-g
#OPTION=-O3

all: main.o stomp.o
	gcc $(OPTION) main.o stomp.o -o main -lpthread

main.o: main.c stomp.h
	gcc $(OPTION) main.c -c

stomp.o: stomp.c stomp.h
	gcc $(OPTION) stomp.c -c

clean:
	rm -rf *.o main
