# Target: test_assign1 depends on object files and is built using gcc
test_assign1: test_assign1_1.o storage_mgr.o dberror.o
	gcc -o test_assign1 test_assign1_1.o storage_mgr.o dberror.o
	
# Target: test_assign1_1.o depends on source file test_assign1_1.c and is compiled using gcc
test_assign1_1.o: test_assign1_1.c
	gcc -c test_assign1_1.c
	
# Target: storage_mgr.o depends on source file storage_mgr.c and is compiled using gcc
storage_mgr.o: storage_mgr.c
	gcc -c storage_mgr.c

# Target: dberror.o depends on source file dberror.c and is compiled using gcc
dberror.o: dberror.c
	gcc -c dberror.c
	
# Target: run depends on test_assign1 and executes it
run: test_assign1
	./test_assign1
	
# Target: clean removes the compiled executable and object files
clean:
	-rm test_assign1 test_assign1_1.o storage_mgr.o dberror.o
