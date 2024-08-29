CC = gcc
CFLAGS = -g -I -Wall
O = -o
TA = test_assign4
RM = rm
RF = -rf

all: $(TA)

$(TA): test_helper.h $(TA)_1.c storage_mgr.h storage_mgr.c dberror.h dberror.c buffer_mgr.h buffer_mgr.c buffer_mgr_stat.h buffer_mgr_stat.c record_mgr.h record_mgr.c expr.h expr.c rm_serializer.c btree_mgr.h btree_mgr.c
	$(CC) $(CFLAGS) dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c record_mgr.c btree_mgr.c $(TA)_1.c $(O) $(TA)

run:
	./$(TA)

clean:
	$(RM) $(RF) $(TA)