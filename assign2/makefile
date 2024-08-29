all: test_assign2_1

test_assign2_1: test_helper.h test_assign2_1.c storage_mgr.h storage_mgr.c dt.h dberror.h dberror.c buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h buffer_mgr.c
	gcc -g -I -Wall dberror.c storage_mgr.c test_assign2_1.c buffer_mgr.c buffer_mgr_stat.c -o test_assign2_1

# Target: run executes test_assign1
run:
	./test_assign2_1

# Target: clean removes the compiled executable
clean:
	rm -rf test_assign2_1