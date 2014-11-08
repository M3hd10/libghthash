#include <stdio.h>
#include <ght_hash_table.h>
#include <memory_mng.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>

#define MAX_BUCKETS 3145728

#define ITHREAD_NUM 20
#define MAX_INSERT 157286

#define ITTHREAD_NUM 35

#define DTHREAD_NUM 20
#define MAX_DELETE 157286

/**
 * global variables
 */
pthread_t ithreads[ITHREAD_NUM];

pthread_t itthreads[ITTHREAD_NUM];

pthread_t dthreads[DTHREAD_NUM];

int insert_count = 0;
int update_count = 0;
int iterator_count = 0;
int iterate_count = 0;
int delete_count = 0;
int failCounter = 0;
int lastItems = 0;
int lastDelete = 0;
int del1_count = 0;
int del2_count = 0;
/**
 * Prototypes
 */
void* insert(void* unused);
void* iteration(void* unused);
void* delete2(void* unused);
void debug_function(void);
void debug_delete(void);

struct hash_data {
	u_int32_t last_timestamp;
};

HASH_MEM_ACCESS_LAYER_ST hash;

void *my_alloc(size_t size) {
	int index = lockless_alloc_memory(&hash.memory_manager);
	ght_hash_entry_t *static_memory =  (ght_hash_entry_t *) hash.static_memory;
	if(index != -1){
		return &static_memory[index];
	}
	return NULL;
}

void my_free(void *p) {
	int index =  ((ght_hash_entry_t *) p ) - ( (ght_hash_entry_t *) hash.static_memory );
	lockless_dealloc_memory(&hash.memory_manager, index);
}

void* insert(void* unused) {
	int threadid = unused;
	int i=0;
	struct hash_data *data = NULL;
	int *key = calloc(1, sizeof(int));
	struct hash_data *org_data = calloc(1, sizeof(struct hash_data));
	for(i=1; i <= MAX_INSERT; i++) {
		*key = i + (threadid * MAX_INSERT);
		// *key = i;
		data = (struct hash_data *)lockless_ght_get(hash.p_table, sizeof(int), key );
		if(data == NULL) {
			//data = calloc(1, sizeof(struct hash_data));
			org_data->last_timestamp = time(NULL);
			lockless_ght_insert(hash.p_table, org_data, sizeof(int), key);
		}
		else {
			data->last_timestamp = time(NULL);
			FAA(&update_count, 1);
		}
	}
	return 0;
}

void* iteration(void *unused) {
	int thread_id = unused;
	lockless_ght_iterator_t iterator;
	iterator.type = HASH_ITERATOR_SKIP_ENTRY;
	struct hash_data *p_data;
  	struct hash_data *temp;
  	void *p_key;
	int counter=0;
	// sleep(3);
	while(1) {
		for (p_data = (struct hash_data *)lockless_ght_first(hash.p_table, &iterator, &p_key); p_data;
		        p_data = (struct hash_data *)lockless_ght_next(hash.p_table, &iterator, &p_key)) {
				
			// if(time(NULL) - p_data->last_timestamp > 2) {
				temp = lockless_ght_iterator_remove(hash.p_table, &iterator, &p_key);
				if(temp != NULL) {
					FAA(&iterator_count, 1);
				//	free(temp);
				}
			// }
		}
	}
	return NULL;
}

void* delete2(void *unused) {
	int threadid = unused;
	int i=0;
	int *key = calloc(1, sizeof(int));
	// sleep(3);
	// while(1) {
		// FAA(&del1_count, 1);
		for(i=0; i<=MAX_DELETE; i++) {
			*key = i + (threadid * MAX_DELETE);
			// *key = i;
			// FAA(&del2_count, 1);
			if(lockless_ght_remove(hash.p_table, sizeof(int), key) != NULL ) {
				FAA(&delete_count, 1);
			}
		}
	// }
	return 0;
}

/*
void debug_function() {
	int i=0;
	int k=0;
	ght_hash_entry_t *temp;
	for(i=0; i < hash.p_table->i_size; i++) {
		temp = hash.p_table->pp_entries[i];
		UnMark(&temp);
		if(temp) {
			printf("head(%d):%p ", i, hash.p_table->pp_entries[i]);
			while(temp) {
				printf("prev:%p events:", temp->p_prev);
				k = 0;
				while(temp->event[k] != NULL) {
					printf("%c(%d) ", temp->event[k], temp->eventCnt[k]);
					k++;
				}
				// printf("next:%p older:%p newer:%p ", temp->p_next, temp->p_older, temp->p_newer);
				printf("next:%p ", temp->p_next);
				//printf("prev:%p, mark:%s, next:%p ", temp->p_prev, temp->mark, temp->p_next);
				temp = temp->p_next;
				UnMark( &temp );
			}
			printf("\n");
		}
	}
}
*/

void debug_delete() {

}
int main() {
	hash.p_table = ght_create(MAX_BUCKETS);

//	 init_array_lookup_table( &hash.memory_manager, 12, 10, &hash.static_memory, sizeof(int));
//	 ght_set_alloc(hash.p_table, my_alloc, my_free);


	ght_hash_entry_t *p_e = (ght_hash_entry_t *)hash.static_memory;
	int i=0;
	for(i=0;i<hash.memory_manager.limit_size;i++){
		if(p_e[i].key.p_key == NULL){
			printf("Can Not Allocated Static Memory For Key index:%d \n", i);
			exit(0);
		}
	}


	// if(hash.static_memory == NULL)
	// 	printf("WOWOOWOWOWOW\n");

	time_t start = time( NULL );
	for(i=0; i<ITHREAD_NUM; i++)
		pthread_create(&ithreads[i], NULL, &insert, i);
	for(i=0; i<ITTHREAD_NUM; i++)
		pthread_create(&itthreads[i], NULL, &iteration, i);
	for(i=0; i<DTHREAD_NUM; i++)
		pthread_create(&dthreads[i], NULL, &delete2, i);	
	while(1) {
		lastItems = hash.p_table->i_items;
		lastDelete = delete_count;
		ght_size(hash.p_table);
		//printf("num of item in hash_table: %d hash_bucket_items:%d\n", hash.p_table->i_items, hash.memory_manager.cur_allocated_num);
		printf("items in hash_table: %d\n", ght_size(hash.p_table));
		printf("items in memory_mng: %d\n", hash.memory_manager.cur_allocated_num);
		printf("deleted items by iterator: %d\n", iterator_count);
		printf("delete count: %d\n", delete_count);
		// printf("del count1: %d\n", del1_count);
		// printf("del count2: %d\n", del2_count);
		//printf("updated count: %d\n", update_count);
		// printf("temp count: %d\n", tempCount);
		// printf("\n");
		// if(lastItems == hash.p_table->i_items && lastItems == 0)
		// 	failCounter++;
		// if(failCounter > 7)
		// 	debug_function();
		// if(lastDelete == delete_count && lastDelete != 0)
		// 	failCounter++;
		// if(failCounter > 3)
		// 	debug_delete();
		printf("\n");
	    sleep(1);
	}

	time_t end = time(NULL);

	printf("The elapsed time: %ld sec\n", end - start);
	printf("# of element in hash table %d\n", hash.p_table->i_items);
	return 0;
}