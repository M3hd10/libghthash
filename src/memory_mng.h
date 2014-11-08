#ifndef MEMORY_MNG_H
#define MEMORY_MNG_H

struct __LOCKLESS_STATIC_BUCKET_HASHTABLE_ST__ {
	int max_l1_array_lookup_table_size;
	int max_l2_array_lookup_table_size;
	int max_l3_array_lookup_table_size;
	
	
	u_int64_t *l1_array_lookup_table;
	u_int64_t *l2_array_lookup_table;
	u_int64_t *l3_array_lookup_table;
	
	int limit_size;
	u_int32_t cur_allocated_num;
	u_int32_t max_core;

};
typedef struct __LOCKLESS_STATIC_BUCKET_HASHTABLE_ST__ LOCKLESS_STATIC_BUCKET_HASHTABLE_ST;


struct __HASH_MEM_ACCESS_LAYER_ST__ {
	ght_hash_table_t *p_table;
	LOCKLESS_STATIC_BUCKET_HASHTABLE_ST memory_manager;
	void* static_memory;
};
typedef struct __HASH_MEM_ACCESS_LAYER_ST__ HASH_MEM_ACCESS_LAYER_ST;


void init_array_lookup_table(LOCKLESS_STATIC_BUCKET_HASHTABLE_ST *array_lookup , int max_l1_array_lookup_table_size, int max_core, void **static_memory, int key_size);
int lockless_alloc_memory(LOCKLESS_STATIC_BUCKET_HASHTABLE_ST *array_lookup);
void lockless_dealloc_memory(LOCKLESS_STATIC_BUCKET_HASHTABLE_ST *array_lookup , int index);


#endif