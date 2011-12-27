# ~gid/CS564/Makefile
#
# Makefile for CS564 Minibase project.  Needs GNU make.
#
# Define DEBUGREL for some kind of debugging output (not from us, from
# the original Minibase implementors.)
#
# Warning: make depend overwrites this file.

.PHONY: depend clean backup setup

MAIN=SortMerge

MINIBASE = ..

CC=g++

CFLAGS= -DUNIX -Wall -g

INCLUDES = -I${MINIBASE}/include -I.

LFLAGS= -L. -lsmjoin -lm

SRCS =test_driver.C SMJTester.C main.C sortMerge.C sort.C scan.C btindex_page.C btleaf_page.C btreefilescan.C db.C heapfile.C key.C new_error.C page.C sorted_page.C system_defs.C

OBJS = $(SRCS:.C=.o)

$(MAIN):  $(OBJS)
	 $(CC) $(CFLAGS) $(INCLUDES) $(OBJS) -o $(MAIN) $(LFLAGS)

# Not really "all", but this is useful for setting up the libraries.
all: $(OBJS)

.C.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

depend: $(SRCS)
	makedepend $(INCLUDES) $^

clean:
	rm -f *.o *~ $(MAIN)
	rm -f my_output

backup:
	mkdir bak
	cp Makefile *.[Ch] bak

run:
	rm -f my_output
	./SortMerge > my_output

# Grab the sources for a user who has only the makefile
setup:
	/bin/cp -i $(MINIBASE)/src/*.[Ch] .
	-/bin/cp -i $(MINIBASE)/src/README .

# DO NOT DELETE THIS LINE -- make depend needs it 
