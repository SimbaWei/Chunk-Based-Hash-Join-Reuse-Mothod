.PHONY: all clean doc distclean 

include system.inc

CXX=g++
CPPFLAGS=-Idist/include/
#CPPFLAGS+=-DPREFETCH
#CPPFLAGS+=-DOUTPUT_AGGREGATE
#CPPFLAGS+=-DOUTPUT_WRITE_NT
#CPPFLAGS+=-DOUTPUT_WRITE_NORMAL
CPPFLAGS+=-DOUTPUT_ASSEMBLE
#CPPFLAGS+=-DDEBUG #-DDEBUG2
CXXFLAGS=$(SYSFLAGS)
#CXXFLAGS+=-g -O0 #-Wall
CXXFLAGS+=-O3
LDFLAGS=-Ldist/lib/
LDLIBS=-lconfig++ -lpthread -lbz2

ifeq ($(HOSTTYPE),sparc)
LDLIBS+=-lcpc
CXXFLAGS+=-mcpu=ultrasparc
endif

all: dist reuse-demo

FILES = common/schema.o common/parser.o common/table.o common/loader.o common/page.o  common/hash.h common/hash.cpp\
		algo/algo.h algo/base.cpp algo/hashbase.cpp algo/hashtable.o algo/storage.o\
		joinerfactory.o


clean:
	rm -f *.o
	rm -f common/*.o
	rm -f reuse-demo

distclean: clean
	rm -rf dist

doc: Doxyfile
	doxygen


#multijoin-serial: $(FILES) main-serial.o

reuse-demo: $(FILES) main.o

dist:
	./pre-init.sh