#include "ght_hash_table.h"
#include "memory_mng.h"

void init_array_lookup_table(LOCKLESS_STATIC_BUCKET_HASHTABLE_ST *array_lookup , int max_l1_array_lookup_table_size, int max_core, void **static_memory, int key_size){
	int i = 0 ;
	array_lookup->cur_allocated_num = 0;
	array_lookup->max_core = max_core;
	array_lookup->max_l1_array_lookup_table_size = max_l1_array_lookup_table_size;
	array_lookup->max_l2_array_lookup_table_size = max_l1_array_lookup_table_size * 64;
	array_lookup->max_l3_array_lookup_table_size = max_l1_array_lookup_table_size * 64 * 64; 
	
	array_lookup->l1_array_lookup_table = (u_int64_t*)calloc( array_lookup->max_l1_array_lookup_table_size , sizeof( u_int64_t ) );
	array_lookup->l2_array_lookup_table = (u_int64_t*)calloc( array_lookup->max_l2_array_lookup_table_size , sizeof( u_int64_t ) );
	array_lookup->l3_array_lookup_table = (u_int64_t*)calloc( array_lookup->max_l3_array_lookup_table_size , sizeof( u_int64_t ) );	
	
	
	array_lookup->limit_size = max_l1_array_lookup_table_size * 64 * 64 * 64;

	for ( i = 0 ; i < array_lookup->max_l1_array_lookup_table_size ; i++ )
		array_lookup->l1_array_lookup_table[ i ] = 0xFFFFFFFFFFFFFFFF;
	for ( i = 0 ; i < array_lookup->max_l2_array_lookup_table_size ; i++ )
		array_lookup->l2_array_lookup_table[ i ] = 0xFFFFFFFFFFFFFFFF;
	for ( i = 0 ; i < array_lookup->max_l3_array_lookup_table_size ; i++ )
		array_lookup->l3_array_lookup_table[ i ] = 0xFFFFFFFFFFFFFFFF;



	*static_memory = calloc(array_lookup->limit_size, sizeof(ght_hash_entry_t));
	if(*static_memory == NULL){
		printf("Can Not Allocated Static Memory size:%d\n", array_lookup->limit_size * sizeof(ght_hash_entry_t));
		exit(0);
	}
	ght_hash_entry_t *p_e = *static_memory;
	for(i=0;i<array_lookup->limit_size;i++){
		p_e[i].key.p_key = (void *)calloc(1, sizeof(key_size));
		if(p_e[i].key.p_key == NULL){
			printf("Can Not Allocated Static Memory For Key index:%d size:%d\n", i, key_size);
			exit(0);
		}
	}
}

int lockless_alloc_memory(LOCKLESS_STATIC_BUCKET_HASHTABLE_ST *array_lookup){

	if ( array_lookup->cur_allocated_num > array_lookup->limit_size ) return -1;
	int found =0;
	int index = -1;

	
	
	int l1_buket_index_remainded=-1;
	int l2_buket_index_remainded=-1;
	int l3_buket_index_remainded=-1;
	
	int l2_free_node_index =-1;
	int l3_free_node_index =-1;
	
	u_int64_t cas_old_value;
	u_int64_t cas_new_value;
	
	int l1_loop_index=0;
	int step = array_lookup->max_core;
	fail_alloc_memory :
	l1_loop_index = array_lookup->cur_allocated_num % array_lookup->max_core;
	fail_alloc_memory_2 :
	found =0;
	for ( ; l1_loop_index < array_lookup->max_l1_array_lookup_table_size && !found; l1_loop_index+=step ) {
		if ( 0 != array_lookup->l1_array_lookup_table[ l1_loop_index ] ) {
			l1_buket_index_remainded = ffsll( array_lookup->l1_array_lookup_table[ l1_loop_index ] ) - 1;
			l2_free_node_index = ( l1_loop_index * 64 ) + l1_buket_index_remainded;
			found = 1;
			break;
		}
	}
	if(!found || l2_free_node_index < 0){
		if(array_lookup->cur_allocated_num < array_lookup->limit_size){
			l1_loop_index = 0;
			step=1;
			goto fail_alloc_memory_2;
		}
		return NULL;
	}
	//l1_loop_index-=step;
	// printf(" l1_buket_index_remainded:%d", l1_buket_index_remainded);
	
	fail_alloc_memory_from_L2 :
	cas_old_value = array_lookup->l1_array_lookup_table[ l1_loop_index ];	
	l2_buket_index_remainded = ffsll( array_lookup->l2_array_lookup_table[ l2_free_node_index ] ) - 1;
	// printf(" l2_buket_index_remainded:%d", l2_buket_index_remainded);
	if(l2_buket_index_remainded == -1){
		cas_new_value = cas_old_value & ( 0xFFFFFFFFFFFFFFFF ^ ( ( u_int64_t ) pow( 2 , l1_buket_index_remainded ) ) );
		if(!CAS1(&array_lookup->l1_array_lookup_table[ l1_loop_index ], &cas_old_value , &cas_new_value) ) {
			goto fail_alloc_memory_from_L2;
		}
		goto fail_alloc_memory;	
	}
	
	fail_alloc_memory_from_L3 :
	cas_old_value = array_lookup->l2_array_lookup_table[ l2_free_node_index ];	
	l3_free_node_index = (l2_free_node_index * 64) + l2_buket_index_remainded;	
	l3_buket_index_remainded = ffsll( array_lookup->l3_array_lookup_table[ l3_free_node_index ] ) -1 ;	
	// printf(" l3_buket_index_remainded:%d\n", l3_buket_index_remainded);	
	if(l3_buket_index_remainded == -1){
		cas_new_value = cas_old_value & ( 0xFFFFFFFFFFFFFFFF ^ ( ( u_int64_t ) pow( 2 , l2_buket_index_remainded ) ) );
		if(!CAS1(&array_lookup->l2_array_lookup_table[ l2_free_node_index ], &cas_old_value , &cas_new_value) ){
			goto fail_alloc_memory_from_L3;	
		}
		goto fail_alloc_memory_from_L2;
	}
	
	cas_old_value = array_lookup->l3_array_lookup_table[ l3_free_node_index ];
	cas_new_value = cas_old_value & ( 0xFFFFFFFFFFFFFFFF ^ ( ( u_int64_t ) pow( 2 , l3_buket_index_remainded ) ) );
	if(cas_old_value == cas_new_value)
		goto fail_alloc_memory_from_L3;
	if( !CAS1(&array_lookup->l3_array_lookup_table[ l3_free_node_index ], &cas_old_value , &cas_new_value) ){
		goto fail_alloc_memory_from_L3;
	}
	index = (l3_free_node_index * 64) + l3_buket_index_remainded;
	FAA(&array_lookup->cur_allocated_num,1);

	return index;
}

void lockless_dealloc_memory(LOCKLESS_STATIC_BUCKET_HASHTABLE_ST *array_lookup , int index){
	if(index >= array_lookup->limit_size)
		return;
	int l3_buket_index = ( int ) ( ( double ) index / ( double ) 64 );
	int l3_buket_index_remainded = index % 64;
	int l2_buket_index = ( int ) ( ( double ) l3_buket_index / ( double ) 64 );
	int l2_buket_index_remainded = l3_buket_index % 64;
	
	int l1_buket_index = ( int ) ( ( double ) l2_buket_index / ( double ) 64 );
	int l1_buket_index_remainded = l2_buket_index % 64;
	
	u_int64_t cas_old_value;
	u_int64_t cas_new_value;
	
	u_int64_t t = pow( 2 , l3_buket_index_remainded );
	fail_dealloc_memory_from_L3:
	// array_lookup->l3_array_lookup_table[ l3_buket_index ] = array_lookup->l3_array_lookup_table[ l3_buket_index ] | t;
	cas_old_value = array_lookup->l3_array_lookup_table[ l3_buket_index ];
	cas_new_value = cas_old_value | t;
	if(cas_old_value == cas_new_value)
		return ;
		
	if(!CAS1(&array_lookup->l3_array_lookup_table[ l3_buket_index ], &cas_old_value, &cas_new_value))
		goto fail_dealloc_memory_from_L3;

	t = pow( 2 , l2_buket_index_remainded );
	fail_dealloc_memory_from_L2:
	// array_lookup->l2_array_lookup_table[ l2_buket_index ] = array_lookup->l2_array_lookup_table[ l2_buket_index ] | t;
	cas_old_value = array_lookup->l2_array_lookup_table[ l2_buket_index ];
	cas_new_value = cas_old_value | t;
	if(!CAS1(&array_lookup->l2_array_lookup_table[ l2_buket_index ], &cas_old_value, &cas_new_value))
		goto fail_dealloc_memory_from_L2;

	t = pow( 2 , l1_buket_index_remainded );
	fail_dealloc_memory_from_L1:
	//array_lookup->l1_array_lookup_table[ l1_buket_index ] = array_lookup->l1_array_lookup_table[ l1_buket_index ] | t;
	cas_old_value = array_lookup->l1_array_lookup_table[ l1_buket_index ];
	cas_new_value = cas_old_value | t;
	if(!CAS1(&array_lookup->l1_array_lookup_table[ l1_buket_index ], &cas_old_value, &cas_new_value))
		goto fail_dealloc_memory_from_L1;	
	FAA(&array_lookup->cur_allocated_num,-1);	
}
