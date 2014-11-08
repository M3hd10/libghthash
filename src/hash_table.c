/*********************************************************************
 *
 * Copyright (C) 2001-2005,  Simon Kagstrom
 *
 * Filename:      hash_table.c
 * Description:   The implementation of the hash table (MK2).
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 * 02111-1307, USA.
 *
 * $Id: hash_table.c 15761 2007-07-15 06:08:52Z ska $
 *
 ********************************************************************/

#include <stdlib.h> /* malloc */
#include <stdio.h>  /* perror */
#include <errno.h>  /* errno */
#include <string.h> /* memcmp */
#include <assert.h> /* assert */
#include <time.h>   /* sleep  */
#include <stdint.h>
#include <limits.h>

#include "ght_hash_table.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/* Flags for the elements. This is currently unused. */
#define FLAGS_NONE     0 /* No flags */
#define FLAGS_NORMAL   0 /* Normal item. All user-inserted stuff is normal */
#define FLAGS_INTERNAL 1 /* The item is internal to the hash table */

/* Prototypes */
static inline void transpose(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_entry_t *p_entry);
static inline void move_to_front(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_entry_t *p_entry);
static inline void free_entry_chain(ght_hash_table_t *p_ht, ght_hash_entry_t *p_entry);
static inline ght_hash_entry_t *search_in_bucket(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_key_t *p_key, unsigned char i_heuristics);

static inline void hk_fill(ght_hash_key_t *p_hk, int i_size, const void *p_key);
//static inline ght_hash_entry_t *he_create(ght_hash_table_t *p_ht, void *p_data, unsigned int i_key_size, const void *p_key_data);
ght_hash_entry_t *he_create(ght_hash_table_t *p_ht, void *p_data, unsigned int i_key_size, const void *p_key_data);
ght_hash_entry_t *lockless_he_create(ght_hash_table_t *p_ht, void *p_data, unsigned int i_key_size, const void *p_key_data);
static void he_finalize(ght_hash_table_t *p_ht, ght_hash_entry_t *p_he);

void *get_next_entry(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, ght_hash_entry_t *start_entry);
static void *lockless_set_iterator(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, ght_hash_entry_t *p_uentry, int l_bucket, const void **p_key, unsigned int *size);
void *lockless_ght_iterator_remove(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **p_key);

/* --- private methods --- */



#ifdef __EVENT_DEBUG_MODE__
void __attribute__((noinline)) EVENTS(char x, ght_hash_entry_t *entry)
{
	int len = strlen(entry->event);
	if(len < 99) {
		if(len > 0) {
			if(entry->event[len - 1] == x) {
				if((entry->eventCnt[len - 1] + 1) < INT_MAX)
					entry->eventCnt[len - 1]++;
			}
			else {
				entry->event[len] = x;
				entry->event[len+1] = NULL;
			}
		}
		else if(len == 0) {
			entry->event[len] = x;
			entry->event[len + 1] = NULL;
		}
	}
}
#else 
	#define EVENTS(...)
#endif





/* Move p_entry one up in its list. */
static inline void transpose(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_entry_t *p_entry) {
	/*
	 *  __    __    __    __
	 * |A_|->|X_|->|Y_|->|B_|
	 *             /
	 * =>        p_entry
	 *  __    __/   __    __
	 * |A_|->|Y_|->|X_|->|B_|
	 */
	if (p_entry->p_prev) /* Otherwise p_entry is already first. */
	{
		ght_hash_entry_t *p_x = p_entry->p_prev;
		ght_hash_entry_t *p_a = p_x ? p_x->p_prev : NULL;
		ght_hash_entry_t *p_b = p_entry->p_next;

		if (p_a) {
			p_a->p_next = p_entry;
		} else /* This element is now placed first */
		{
			p_ht->pp_entries[l_bucket] = p_entry;
		}

		if (p_b) {
			p_b->p_prev = p_x;
		}
		if (p_x) {
			p_x->p_next = p_entry->p_next;
			p_x->p_prev = p_entry;
		}
		p_entry->p_next = p_x;
		p_entry->p_prev = p_a;
	}
}

/* Move p_entry first */
static inline void move_to_front(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_entry_t *p_entry) {
	/*
	 *  __    __    __
	 * |A_|->|B_|->|X_|
	 *            /
	 * =>  p_entry
	 *  __/   __    __
	 * |X_|->|A_|->|B_|
	 */
	if (p_entry == p_ht->pp_entries[l_bucket]) {
		return;
	}

	/* Link p_entry out of the list. */
	p_entry->p_prev->p_next = p_entry->p_next;
	if (p_entry->p_next) {
		p_entry->p_next->p_prev = p_entry->p_prev;
	}

	/* Place p_entry first */
	p_entry->p_next = p_ht->pp_entries[l_bucket];
	p_entry->p_prev = NULL;
	p_ht->pp_entries[l_bucket]->p_prev = p_entry;
	p_ht->pp_entries[l_bucket] = p_entry;
}

static inline void remove_from_chain(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_entry_t *p) {
	if (p->p_prev) {
		p->p_prev->p_next = p->p_next;
	} else /* first in list */
	{
		p_ht->pp_entries[l_bucket] = p->p_next;
	}
	if (p->p_next) {
		p->p_next->p_prev = p->p_prev;
	}

	if (p->p_older) {
		p->p_older->p_newer = p->p_newer;
	} else /* oldest */
	{
		p_ht->p_oldest = p->p_newer;
	}
	if (p->p_newer) {
		p->p_newer->p_older = p->p_older;
	} else /* newest */
	{
		p_ht->p_newest = p->p_older;
	}
}

/*
static inline ght_hash_entry_t *lockless_search_in_bucket(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_key_t *p_key, unsigned char i_heuristics) {
	ght_hash_entry_t *p_e = p_ht->pp_entries[l_bucket];
	UnMark(&(p_e));
	while (p_e) {
		if (!Has_Mark(&(p_e->p_next)) && (p_e->key.i_size == p_key->i_size) && (memcmp(p_e->key.p_key, p_key->p_key, p_e->key.i_size) == 0))
			return p_e;
		p_e = p_e->p_next;
		UnMark(&p_e);
	}
	return NULL;
}
*/

static inline ght_hash_entry_t *lockless_search_in_bucket(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_key_t *p_key, unsigned char i_heuristics) {
	ght_hash_entry_t *p_e = p_ht->pp_entries[l_bucket];
	ght_hash_entry_t *p_prev_step = NULL;
	UnMark(&(p_e));

	int refcnt;
	while (p_e) {
		do {
			refcnt = p_e->refCount;
		} while(p_e && !__sync_bool_compare_and_swap(&p_e->refCount, refcnt, refcnt + 2));

		if(p_e->refCount % 2 == 0) { 
			if ( !Has_Mark(&(p_e->p_next)) && (p_e->key.i_size == p_key->i_size) && (memcmp(p_e->key.p_key, p_key->p_key, p_e->key.i_size) == 0)) {
				return p_e;
			}
			FAA(&p_e->refCount, -2);
		}
		p_e = p_e->p_next;
		UnMark( &p_e );
	}
	return NULL;
}

/* Search for an element in a bucket */
static inline ght_hash_entry_t *search_in_bucket(ght_hash_table_t *p_ht, ght_uint32_t l_bucket, ght_hash_key_t *p_key, unsigned char i_heuristics) {
	ght_hash_entry_t *p_e;

	for (p_e = p_ht->pp_entries[l_bucket]; p_e; p_e = p_e->p_next) {
		if ((p_e->key.i_size == p_key->i_size) && (memcmp(p_e->key.p_key, p_key->p_key, p_e->key.i_size) == 0)) {
			/* Matching entry found - Apply heuristics, if any */
			switch (i_heuristics) {
			case GHT_HEURISTICS_MOVE_TO_FRONT:
				move_to_front(p_ht, l_bucket, p_e);
				break;
			case GHT_HEURISTICS_TRANSPOSE:
				transpose(p_ht, l_bucket, p_e);
				break;
			default:
				break;
			}
			return p_e;
		}
	}
	return NULL;
}

/* Free a chain of entries (in a bucket) */
static inline void free_entry_chain(ght_hash_table_t *p_ht, ght_hash_entry_t *p_entry) {
	ght_hash_entry_t *p_e = p_entry;

	while (p_e) {
		ght_hash_entry_t *p_e_next = p_e->p_next;
		he_finalize(p_ht, p_e);
		p_e = p_e_next;
	}
}

/* Fill in the data to a existing hash key */
static inline void hk_fill(ght_hash_key_t *p_hk, int i_size, const void *p_key) {
	assert(p_hk);

	p_hk->i_size = i_size;
	p_hk->p_key = p_key;
}

ght_hash_entry_t *lockless_he_create(ght_hash_table_t *p_ht, void *p_data, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_he;

	if (!(p_he = (ght_hash_entry_t*) p_ht->fn_alloc(sizeof(ght_hash_entry_t) + i_key_size))) {
		fprintf(stderr, "fn_alloc failed!\n");
		return NULL;
	}
	//memset(p_he, 0, sizeof(ght_hash_entry_t) + i_key_size);

	int i=0;
	p_he->p_data = p_data;
	p_he->p_next = NULL;
	p_he->p_prev = NULL;
	p_he->p_older = NULL;
	p_he->p_newer = NULL;
	p_he->refCount = 2;

#ifdef __EVENT_DEBUG_MODE__
	p_he->event[0] = NULL;
	for(i=0; i<100; i++)
		p_he->eventCnt[i] = 0;
#endif

	EVENTS('C', p_he);

	/* Create the key */
	p_he->key.i_size = i_key_size;

	memcpy(p_he->key.p_key, p_key_data, i_key_size);	

	return p_he;
}

/* Create an hash entry */
//static inline ght_hash_entry_t *he_create(ght_hash_table_t *p_ht, void *p_data, unsigned int i_key_size, const void *p_key_data) {
ght_hash_entry_t *he_create(ght_hash_table_t *p_ht, void *p_data, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_he;

	/*
	 * An element like the following is allocated:
	 *        elem->p_key
	 *       /   elem->p_key->p_key_data
	 *  ____|___/________
	 * |elem|key|key data|
	 * |____|___|________|
	 *
	 * That is, the key and the key data is stored "inline" within the
	 * hash entry.
	 *
	 * This saves space since malloc only is called once and thus avoids
	 * some fragmentation. Thanks to Dru Lemley for this idea.
	 */
	if (!(p_he = (ght_hash_entry_t*) p_ht->fn_alloc(sizeof(ght_hash_entry_t) + i_key_size))) {
		fprintf(stderr, "fn_alloc failed!\n");
		return NULL;
	}
	memset(p_he, 0, sizeof(ght_hash_entry_t) + i_key_size);

	int i=0;
	p_he->p_data = p_data;
	p_he->p_next = NULL;
	p_he->p_prev = NULL;
	p_he->p_older = NULL;
	p_he->p_newer = NULL;
	p_he->refCount = 2;

	/* Create the key */
	p_he->key.i_size = i_key_size;
	memcpy(p_he + 1, p_key_data, i_key_size);
	p_he->key.p_key = (void*) (p_he + 1);
	return p_he;
}

/* Finalize (free) a hash entry */
static void __attribute__((noinline)) he_finalize(ght_hash_table_t *p_ht, ght_hash_entry_t *p_he) {
	assert(p_he);

#if !defined(NDEBUG)
	p_he->p_next = NULL;
	p_he->p_prev = NULL;
	p_he->p_older = NULL;
	p_he->p_newer = NULL;
#endif /* NDEBUG */

	while(!__sync_bool_compare_and_swap(&p_he->refCount, 2, 1));

	p_he->p_data = NULL;
	p_he->p_prev = 0x1;
	p_he->p_next = 0x1;

	EVENTS('F', p_he);
	p_ht->fn_free(p_he);
}

#if 0
/* Tried this to avoid recalculating hash values by caching
 * them. Overhead larger than benefits.
 */
static inline ght_uint32_t get_hash_value(ght_hash_table_t *p_ht, ght_hash_key_t *p_key)
{
	int i;

	if (p_key->i_size > sizeof(uint64_t))
	return p_ht->fn_hash(p_key);

	/* Lookup in the hash value cache */
	for (i = 0; i < GHT_N_CACHED_HASH_KEYS; i++)
	{
		if ( p_key->i_size == p_ht->cached_keys[i].key.i_size &&
				memcmp(p_key->p_key, p_ht->cached_keys[i].key.p_key, p_key->i_size) == 0)
		return p_ht->cached_keys[i].hash_val;
	}
	p_ht->cur_cache_evict = (p_ht->cur_cache_evict + 1) % GHT_N_CACHED_HASH_KEYS;
	p_ht->cached_keys[ p_ht->cur_cache_evict ].key = *p_key;
	p_ht->cached_keys[ p_ht->cur_cache_evict ].hash_val = p_ht->fn_hash(p_key);

	return p_ht->cached_keys[ p_ht->cur_cache_evict ].hash_val;
}
#else
# define get_hash_value(p_ht, p_key) ( (p_ht)->fn_hash(p_key) )
#endif

/* --- Exported methods --- */
/* Create a new hash table */
ght_hash_table_t *ght_create(unsigned int i_size) {
	ght_hash_table_t *p_ht;
	int i = 1;

	if (!(p_ht = (ght_hash_table_t*) malloc(sizeof(ght_hash_table_t)))) {
		perror("malloc");
		return NULL;
	}

	/* Set the size of the hash table to the nearest 2^i higher then i_size */
	p_ht->i_size = 1;
	while (p_ht->i_size < i_size) {
		p_ht->i_size = 1 << i++;
	}

	p_ht->i_size_mask = (1 << (i - 1)) - 1; /* Mask to & with */
	p_ht->i_items = 0;

	p_ht->fn_hash = ght_one_at_a_time_hash;

	/* Standard values for allocations */
	p_ht->fn_alloc = malloc;
	p_ht->fn_free = free;

	/* Set flags */
	p_ht->i_heuristics = GHT_HEURISTICS_NONE;
	p_ht->i_automatic_rehash = FALSE;

	p_ht->bucket_limit = 0;
	p_ht->fn_bucket_free = NULL;
	p_ht->mem_type = HASH_DYNAMIC_MEM;

	/* Create an empty bucket list. */
	if (!(p_ht->pp_entries = (ght_hash_entry_t**) malloc(p_ht->i_size * sizeof(ght_hash_entry_t*)))) {
		perror("malloc");
		free(p_ht);
		return NULL;
	}
	memset(p_ht->pp_entries, 0, p_ht->i_size * sizeof(ght_hash_entry_t*));

	/* Initialise the number of entries in each bucket to zero */
	if (!(p_ht->p_nr = (unsigned int*) malloc(p_ht->i_size * sizeof(unsigned int)))) {
		perror("malloc");
		free(p_ht->pp_entries);
		free(p_ht);
		return NULL;
	}
	memset(p_ht->p_nr, 0, p_ht->i_size * sizeof(int));

	p_ht->p_oldest = NULL;
	p_ht->p_newest = NULL;
	
	return p_ht;
}

/* Set the allocation/deallocation function to use */
void ght_set_alloc(ght_hash_table_t *p_ht, ght_fn_alloc_t fn_alloc, ght_fn_free_t fn_free) {
	p_ht->fn_alloc = fn_alloc;
	p_ht->fn_free = fn_free;
	p_ht->mem_type = HASH_STATIC_MEM;
}

/* Set the hash function to use */
void ght_set_hash(ght_hash_table_t *p_ht, ght_fn_hash_t fn_hash) {
	p_ht->fn_hash = fn_hash;
}

/* Set the heuristics to use. */
void ght_set_heuristics(ght_hash_table_t *p_ht, int i_heuristics) {
	p_ht->i_heuristics = i_heuristics;
}

/* Set the rehashing status of the table. */
void ght_set_rehash(ght_hash_table_t *p_ht, int b_rehash) {
	p_ht->i_automatic_rehash = b_rehash;
}

void ght_set_bounded_buckets(ght_hash_table_t *p_ht, unsigned int limit, ght_fn_bucket_free_callback_t fn) {
	p_ht->bucket_limit = limit;
	p_ht->fn_bucket_free = fn;

	if (limit > 0 && fn == NULL) {
		fprintf(stderr, "ght_set_bounded_buckets: The bucket callback function is NULL but the limit is %d\n", limit);
	}
}

/* Get the number of items in the hash table */
unsigned int ght_size(ght_hash_table_t *p_ht) {
	return p_ht->i_items;
}

/* Get the size of the hash table */
unsigned int ght_table_size(ght_hash_table_t *p_ht) {
	return p_ht->i_size;
}

/* Insert an entry into the hash table without use of lock */
int lockless_ght_insert(ght_hash_table_t *p_ht, void *p_entry_data, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_entry;
	ght_uint32_t l_key;
	ght_hash_key_t key;
	ght_hash_entry_t *p_ret;
	ght_hash_entry_t *p_unext;

	assert(p_ht);
	
	hk_fill(&key, i_key_size, p_key_data);

	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

	if(p_ht->mem_type == HASH_STATIC_MEM ){
		if (!(p_entry = lockless_he_create(p_ht, p_entry_data, i_key_size, p_key_data)))
			return -2;
	}
	else{
		if (!(p_entry = he_create(p_ht, p_entry_data, i_key_size, p_key_data)))
			return -2;		
	}

	fail_ins1:
	p_ret = lockless_search_in_bucket(p_ht, l_key, &key, 0);
	if (p_ret) {
		FAA(&p_ret->refCount, -2);
		he_finalize(p_ht, p_entry);
		return -1;
	}
	
	p_entry->p_next = p_ht->pp_entries[l_key];
	UnMark( &(p_entry->p_next) );
	Mark_delete( &p_entry->p_next );
	p_unext = p_entry->p_next;
	Unmark_delete( &p_unext );
	p_entry->p_prev = NULL;

	if(!CAS1(&p_ht->pp_entries[l_key], &p_unext, &p_entry))
		goto fail_ins1;
	
	fail_ins2:
	if(p_unext != NULL) {
		if( !CAS2(&p_unext->p_prev, NULL, &p_entry) )
			goto fail_ins2;
	}
	
	Unmark_delete( &p_entry->p_next );
	FAA(&p_entry->refCount, -2);
	EVENTS('Z', p_entry);

	FAA(&(p_ht->p_nr[l_key]), 1);
	FAA(&(p_ht->i_items), 1);

	return 0;
}

/* Insert an entry into the hash table without use of lock */
// int lockless_ght_insert(ght_hash_table_t *p_ht, void *p_entry_data, unsigned int i_key_size, const void *p_key_data) {
// 	ght_hash_entry_t *p_entry;
// 	ght_uint32_t l_key;
// 	ght_hash_key_t key;
// 	ght_hash_entry_t *p_ret;
// 	ght_hash_entry_t *p_unext;

// 	assert(p_ht);

// 	//hk_fill need not to be atomic.
	
// 	hk_fill(&key, i_key_size, p_key_data);

// 	//warning : the i_size_mask value may be changed during read
// 	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

// 	//warning : the he_create must be checked again!
// 	if (!(p_entry = lockless_he_create(p_ht, p_entry_data, i_key_size, p_key_data))) {
// 		return -2;
// 	}

// 	fail_ins:
// 	//we must use lockless search on bucket
// 	p_ret = lockless_search_in_bucket(p_ht, l_key, &key, 0);
// 	if (p_ret) {
// 		FAA(&p_ret->refCount, -2);
// 		he_finalize(p_ht, p_entry);
// 		return -1;
// 	}
	
// 	EVENTS('1', p_entry);
// 	//init next and prev pointer of new entry
// 	p_entry->p_next = p_ht->pp_entries[l_key];
// 	UnMark( &(p_entry->p_next) );
// 	Mark_delete( &p_entry->p_next );
// 	p_unext = p_entry->p_next;
// 	UnMark( &p_unext );
// 	p_entry->p_prev = NULL;

// 	//if (p_ht->pp_entries[l_key] == NULL) {
// 	// if(p_entry->p_next == NULL) {
// 	if(p_unext == NULL) {
// 		EVENTS('2', p_entry);
// 		if (!CAS2(&(p_ht->pp_entries[l_key]), (void *) 0, &p_entry)) {
// 			EVENTS('3', p_entry);
// 			goto fail_ins;
// 		}
// 	} else {
// 		EVENTS('4', p_entry);
// 		// if (!CAS1( &(p_ht->pp_entries[l_key]), &(p_entry->p_next), &p_entry)) {
// 		if (!CAS1( &(p_ht->pp_entries[l_key]), &p_unext, &p_entry)) {
// 			EVENTS('5', p_entry);
// 			goto fail_ins;
// 		}
// 		// p_unext = p_entry->p_next;

// 		fail_backward_link:
// 		if (p_unext != NULL) {
// 			EVENTS('6', p_entry);
// 			if(!CAS2(&p_unext->p_prev, NULL, &p_entry) )
// 				goto fail_backward_link;
// 			EVENTS('J', p_unext);
// 			p_unext->p_older = p_entry;
// 		}
// 	}
// 	// p_entry->p_prev = NULL;
// 	Unmark_delete( &p_entry->p_next );
// 	FAA(&p_entry->refCount, -2);
// 	EVENTS('I', p_entry);
// 	FAA(&(p_ht->p_nr[l_key]), 1);

// 	FAA(&(p_ht->i_items), 1);

// 	return 0;
// }

/* Insert an entry into the hash table */
int ght_insert(ght_hash_table_t *p_ht, void *p_entry_data, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_entry;
	ght_uint32_t l_key;
	ght_hash_key_t key;

	assert(p_ht);

	hk_fill(&key, i_key_size, p_key_data);
	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;
	if (search_in_bucket(p_ht, l_key, &key, 0)) {
		/* Don't insert if the key is already present. */
		return -1;
	}
	if (!(p_entry = he_create(p_ht, p_entry_data, i_key_size, p_key_data))) {
		return -2;
	}

	/* Rehash if the number of items inserted is too high. */
	if (p_ht->i_automatic_rehash && p_ht->i_items > 2 * p_ht->i_size) {
		ght_rehash(p_ht, 2 * p_ht->i_size);
		/* Recalculate l_key after ght_rehash has updated i_size_mask */
		l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;
	}

	/* Place the entry first in the list. */
	p_entry->p_next = p_ht->pp_entries[l_key];
	p_entry->p_prev = NULL;
	if (p_ht->pp_entries[l_key]) {
		p_ht->pp_entries[l_key]->p_prev = p_entry;
	}
	p_ht->pp_entries[l_key] = p_entry;

	/* If this is a limited bucket hash table, potentially remove the last item */
	if (p_ht->bucket_limit != 0 && p_ht->p_nr[l_key] >= p_ht->bucket_limit) {
		ght_hash_entry_t *p;

		/* Loop through entries until the last
		 *
		 * FIXME: Better with a pointer to the last entry
		 */
		for (p = p_ht->pp_entries[l_key]; p->p_next != NULL; p = p->p_next)
			;

		assert(p && p->p_next == NULL);

		remove_from_chain(p_ht, l_key, p); /* To allow it to be reinserted in fn_bucket_free */
		p_ht->fn_bucket_free(p->p_data, p->key.p_key);

		he_finalize(p_ht, p);
	} else {
		p_ht->p_nr[l_key]++;

		assert(p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1);

		p_ht->i_items++;
	}

	if (p_ht->p_oldest == NULL) {
		p_ht->p_oldest = p_entry;
	}
	p_entry->p_older = p_ht->p_newest;

	if (p_ht->p_newest != NULL) {
		p_ht->p_newest->p_newer = p_entry;
	}

	p_ht->p_newest = p_entry;

	return 0;
}

/* Get an entry from the hash table. The entry is returned, or NULL if it wasn't found */
void *lockless_ght_get(ght_hash_table_t *p_ht, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_e;
	ght_hash_key_t key;
	ght_uint32_t l_key;

	assert(p_ht);

	hk_fill(&key, i_key_size, p_key_data);

	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

	/* Check that the first element in the list really is the first. */
	assert(p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1);

	p_e = lockless_search_in_bucket(p_ht, l_key, &key, p_ht->i_heuristics);
	if(p_e) {
		FAA(&p_e->refCount, -2);
		EVENTS('K', p_e);
	}
	return (p_e ? p_e->p_data : NULL);
}

/* Get an entry from the hash table. The entry is returned, or NULL if it wasn't found */
void *ght_get(ght_hash_table_t *p_ht, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_e;
	ght_hash_key_t key;
	ght_uint32_t l_key;

	assert(p_ht);

	hk_fill(&key, i_key_size, p_key_data);

	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

	/* Check that the first element in the list really is the first. */
	assert(p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1);

	/* LOCK: p_ht->pp_entries[l_key] */
	p_e = search_in_bucket(p_ht, l_key, &key, p_ht->i_heuristics);
	/* UNLOCK: p_ht->pp_entries[l_key] */

	return (p_e ? p_e->p_data : NULL);
}

/* Replace an entry from the hash table. The entry is returned, or NULL if it wasn't found */
void *ght_replace(ght_hash_table_t *p_ht, void *p_entry_data, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_e;
	ght_hash_key_t key;
	ght_uint32_t l_key;
	void *p_old;

	assert(p_ht);

	hk_fill(&key, i_key_size, p_key_data);

	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

	/* Check that the first element in the list really is the first. */
	assert(p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1);

	/* LOCK: p_ht->pp_entries[l_key] */
	p_e = search_in_bucket(p_ht, l_key, &key, p_ht->i_heuristics);
	/* UNLOCK: p_ht->pp_entries[l_key] */

	if (!p_e)
		return NULL;

	p_old = p_e->p_data;
	p_e->p_data = p_entry_data;

	return p_old;
}

void *lockless_ght_remove(ght_hash_table_t *p_ht, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_out;
	ght_hash_key_t key;
	ght_uint32_t l_key;
	void *p_ret = NULL;
	ght_hash_entry_t *p_unext = NULL;
	ght_hash_entry_t *p_uprev = NULL;

	assert(p_ht);

	hk_fill(&key, i_key_size, p_key_data);
	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

	/* Check that the first element really is the first */
	assert((p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL : 1));

	fail_del: p_out = lockless_search_in_bucket(p_ht, l_key, &key, 0);
	if (p_out && p_out->p_data != NULL) {
		EVENTS('a', p_out);
		if (!Mark_delete(&(p_out->p_next))) {
			FAA(&p_out->refCount, -2);
			goto fail_del;
		}
		EVENTS('b', p_out);
		p_unext = p_out->p_next;
		while(!UnMark( &p_unext ));
		
		if(!Mark_delete(&(p_out->p_prev))) {
			FAA(&p_out->refCount, -2);
			while(!Unmark_delete( &(p_out->p_next)));
			goto fail_del;
		}
		p_uprev = p_out->p_prev;
		while(!UnMark( &p_uprev ));
		EVENTS('c', p_out);
		if (p_uprev != NULL) {
			EVENTS('d', p_out);
			if (!CAS1(&(p_uprev->p_next), &p_out, &p_unext)) {
				EVENTS('e', p_out);
				FAA(&p_out->refCount, -2);
				while(!Unmark_delete(&(p_out->p_prev)));
				while(!Unmark_delete(&(p_out->p_next)));
				goto fail_del;
			}
		} else {
			EVENTS('f', p_out);
			if (!CAS1(&(p_ht->pp_entries[l_key]), &p_out, &p_unext)) {
				EVENTS('g', p_out);
				FAA(&p_out->refCount, -2);
				while(!Unmark_delete(&(p_out->p_prev)));
				while(!Unmark_delete(&(p_out->p_next)));
				goto fail_del;
			}
		}

		fail_nxt_rem: if (p_unext != NULL) {
			EVENTS('h', p_out);
			if (!CAS1( &(p_unext->p_prev), &p_out, &p_uprev) ) {
				goto fail_nxt_rem;
			}
		}
		
#if !defined(NDEBUG)
		p_out->p_next = NULL;
		p_out->p_prev = NULL;
#endif /* NDEBUG */


		FAA(&(p_ht->i_items), -1);

		FAA(&(p_ht->p_nr[l_key]), -1);
		p_ret = p_out->p_data;
//		p_out->p_data = NULL;
//		p_out->p_prev = 0x1;
//		p_out->p_next = 0x1;
		p_out->p_older = p_uprev;
		p_out->p_newer = p_unext;
		EVENTS('R', p_out);
		he_finalize(p_ht, p_out);
	}
	else if( p_out && p_out->p_data == NULL )
		FAA(&p_out->refCount, -2);

	return p_ret;
}

/* Remove an entry from the hash table. The removed entry, or NULL, is
 returned (and NOT free'd). */
void *ght_remove(ght_hash_table_t *p_ht, unsigned int i_key_size, const void *p_key_data) {
	ght_hash_entry_t *p_out;
	ght_hash_key_t key;
	ght_uint32_t l_key;
	void *p_ret = NULL;

	assert(p_ht);

	hk_fill(&key, i_key_size, p_key_data);
	l_key = get_hash_value(p_ht, &key) & p_ht->i_size_mask;

	/* Check that the first element really is the first */
	assert((p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1));

	/* LOCK: p_ht->pp_entries[l_key] */
	p_out = search_in_bucket(p_ht, l_key, &key, 0);

	/* Link p_out out of the list. */
	if (p_out) {
		remove_from_chain(p_ht, l_key, p_out);

		/* This should ONLY be done for normal items (for now all items) */
		p_ht->i_items--;

		p_ht->p_nr[l_key]--;
		/* UNLOCK: p_ht->pp_entries[l_key] */
#if !defined(NDEBUG)
		p_out->p_next = NULL;
		p_out->p_prev = NULL;
#endif /* NDEBUG */

		p_ret = p_out->p_data;
		he_finalize(p_ht, p_out);
	}
	/* else: UNLOCK: p_ht->pp_entries[l_key] */

	return p_ret;
}

static inline void *first_keysize(ght_hash_table_t *p_ht, ght_iterator_t *p_iterator, const void **pp_key, unsigned int *size) {
	assert(p_ht && p_iterator);

	/* Fill the iterator */
	p_iterator->p_entry = p_ht->p_oldest;

	if (p_iterator->p_entry) {
		p_iterator->p_next = p_iterator->p_entry->p_newer;
		*pp_key = p_iterator->p_entry->key.p_key;
		if (size != NULL)
			*size = p_iterator->p_entry->key.i_size;

		return p_iterator->p_entry->p_data;
	}

	p_iterator->p_next = NULL;
	*pp_key = NULL;
	if (size != NULL)
		*size = 0;

	return NULL;
}

/* Get the first entry in an iteration */
void *ght_first(ght_hash_table_t *p_ht, ght_iterator_t *p_iterator, const void **pp_key) {
	return first_keysize(p_ht, p_iterator, pp_key, NULL);
}

void *ght_first_keysize(ght_hash_table_t *p_ht, ght_iterator_t *p_iterator, const void **pp_key, unsigned int *size) {
	return first_keysize(p_ht, p_iterator, pp_key, size);
}

static inline void *next_keysize(ght_hash_table_t *p_ht, ght_iterator_t *p_iterator, const void **pp_key, unsigned int *size) {
	assert(p_ht && p_iterator);

	if (p_iterator->p_next) {
		/* More entries */
		p_iterator->p_entry = p_iterator->p_next;
		p_iterator->p_next = p_iterator->p_next->p_newer;

		*pp_key = p_iterator->p_entry->key.p_key;
		if (size != NULL)
			*size = p_iterator->p_entry->key.i_size;

		return p_iterator->p_entry->p_data; /* We know that this is non-NULL */
	}

	/* Last entry */
	p_iterator->p_entry = NULL;
	p_iterator->p_next = NULL;

	*pp_key = NULL;
	if (size != NULL)
		*size = 0;

	return NULL;
}

/* Get the next entry in an iteration. You have to call ght_first
 once initially before you use this function */
void *ght_next(ght_hash_table_t *p_ht, ght_iterator_t *p_iterator, const void **pp_key) {
	return next_keysize(p_ht, p_iterator, pp_key, NULL);
}

void *ght_next_keysize(ght_hash_table_t *p_ht, ght_iterator_t *p_iterator, const void **pp_key, unsigned int *size) {
	return next_keysize(p_ht, p_iterator, pp_key, size);
}

/*
 * this function try to mark an entry on the bucket which its head has been iteration_mark!
 */
void *get_next_entry(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, ght_hash_entry_t *start_entry) {
	ght_hash_entry_t *p_utemp = NULL;
	
	p_utemp = start_entry;
	UnMark( &p_utemp );
	if(p_utemp) {
		switch(p_iterator->type)
		{
			case HASH_ITERATOR_WAIT:
				while(p_utemp && !Mark_iteration( &(p_utemp->p_next) )) {
					usleep(1);
					p_utemp = p_iterator->p_next;
					UnMark( &p_utemp );					
				}
				return p_utemp;
			break;

			case HASH_ITERATOR_SKIP_ENTRY:
				while(p_utemp && !Mark_iteration( &(p_utemp->p_next) ))
				{
					p_utemp = p_utemp->p_next;
					UnMark( &p_utemp );
					p_iterator->jumpCounter++;
				}
				return p_utemp;
			break;

			default:
				if(Mark_iteration( &(p_utemp->p_next) ))
					return p_utemp;
				else
					return NULL;
			break;
		}
	}
	return NULL;
}

void *get_next_entry2(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, ght_hash_entry_t *start_entry) {
	ght_hash_entry_t *p_utemp = NULL;
	
	p_utemp = start_entry;
	UnMark( &p_utemp );
	if(p_utemp) {
		switch(p_iterator->type)
		{
			case HASH_ITERATOR_WAIT:
				while(p_utemp && !Mark_iteration( &(p_utemp->p_next) ))
					usleep(1);
				return p_utemp;
			break;

			case HASH_ITERATOR_SKIP_ENTRY:
				while(p_utemp && !Mark_iteration( &(p_utemp->p_next) ))
				{
					p_utemp = p_utemp->p_next;
					UnMark( &p_utemp );
					p_iterator->jumpCounter++;
				}
				return p_utemp;
			break;

			default:
				if(Mark_iteration( &(p_utemp->p_next) ))
					return p_utemp;
				else
					return NULL;
			break;
		}
	}
	return NULL;
}

static void *lockless_set_iterator(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, ght_hash_entry_t *p_uentry, int iterator_bucket, const void **p_key, unsigned int *size) {

	p_iterator->p_entry = p_uentry;
	p_iterator->p_next = p_uentry->p_next;
	UnMark_iteration( &(p_iterator->p_next) );
	*p_key = p_iterator->p_entry->key.p_key;
	if (size != NULL)
		*size = p_iterator->p_entry->key.i_size;
	p_iterator->next_ibucket = iterator_bucket + 1;
}

static void *lockless_first_keysize(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **p_key, unsigned int *size) {
	
	assert(p_ht && p_iterator);
	
	p_iterator->jumpCounter = 0;
	p_iterator->p_entry = NULL;
	p_iterator->p_next = NULL;
	p_iterator->next_ibucket = 0;
	p_iterator->was_forwarded_by_delete = 'n';
	
	ght_hash_entry_t *p_uhead = NULL;
	ght_hash_entry_t *p_uentry = NULL;
	
	int i = 0;
	for(i = 0; i < p_ht->i_size && !p_uentry; i++)
	{
		p_uhead = p_ht->pp_entries[i];
		UnMark( &p_uhead );
		if(p_uhead == NULL)
			continue;
		if( Mark_iteration( &(p_ht->pp_entries[i]) ))
		{
			p_uentry = get_next_entry(p_ht, p_iterator, p_ht->pp_entries[i]);
			if( p_uentry ) {
				lockless_set_iterator(p_ht, p_iterator, p_uentry, i, p_key, size);
			}
			UnMark_iteration( &(p_ht->pp_entries[i]) );
		}
		else
		{	
			p_uentry = get_next_entry(p_ht, p_iterator, p_ht->pp_entries[i]);
			if(p_uentry)
				lockless_set_iterator(p_ht, p_iterator, p_uentry, i, p_key, size);
		}
	}
	if(p_uentry) {
		EVENTS('M', p_uentry);
		return p_iterator->p_entry->p_data;
	}
	p_iterator->p_entry = NULL;
	p_iterator->p_next = NULL;
	p_iterator->next_ibucket = i;
	*p_key = NULL;
	if (size != NULL)
		*size = 0;
	return NULL;
}

void *lockless_ght_first(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **p_key) {
	return lockless_first_keysize(p_ht, p_iterator, p_key, NULL);
}

void *lockless_ght_first_keysize(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **p_key, unsigned int *size) {
	return lockless_first_keysize(p_ht, p_iterator, p_key, size);
}

static void *lockless_next_keysize(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **p_key, unsigned int *size) {
	assert(p_ht && p_iterator);
	
	ght_hash_entry_t *p_uentry = NULL;
	ght_hash_entry_t *p_uhead = NULL;

	int i = 0;

	if (p_iterator->was_forwarded_by_delete == 'n') {
		p_uentry = get_next_entry(p_ht, p_iterator, p_iterator->p_next);
		if(p_uentry)
		{
			UnMark_iteration( &(p_iterator->p_entry->p_next) );
			lockless_set_iterator(p_ht, p_iterator, p_uentry, p_iterator->next_ibucket - 1, p_key, size);
		}
		else
		{
			for (i = p_iterator->next_ibucket; i < p_ht->i_size && !p_uentry; i++)
			{
				p_uhead = p_ht->pp_entries[i];
				UnMark( &p_uhead );
				if(p_uhead == NULL)
					continue;

				if(Mark_iteration( &(p_ht->pp_entries[i] )))
				{
					p_uentry = get_next_entry(p_ht, p_iterator, p_ht->pp_entries[i]);
					if(p_uentry)
					{
						UnMark_iteration( &(p_iterator->p_entry->p_next) );
						lockless_set_iterator(p_ht, p_iterator, p_uentry, i, p_key, size);
					}
					UnMark( &(p_ht->pp_entries[i]) );
				}
				else
				{
					p_uentry = get_next_entry(p_ht, p_iterator, p_ht->pp_entries[i]);
					if(p_uentry){
						UnMark_iteration( &(p_iterator->p_entry->p_next) );
						lockless_set_iterator(p_ht, p_iterator, p_uentry, i, p_key, size);
					}
				}
			}
		}
		if(p_uentry) {
			EVENTS('N', p_uentry);
			return p_iterator->p_entry->p_data;
		}
		UnMark_iteration( &(p_iterator->p_entry->p_next) );
		p_iterator->p_entry = NULL;
		p_iterator->p_next = NULL;
		p_iterator->next_ibucket = i;
		*p_key = NULL;
		if (size != NULL)
			*size = 0;
	}
	else {
		p_iterator->was_forwarded_by_delete = 'n';
		if (p_iterator->p_entry != NULL)
			return p_iterator->p_entry->p_data;
	}
	return NULL;
}

void *lockless_ght_next(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **pp_key) {
	return lockless_next_keysize(p_ht, p_iterator, pp_key, NULL);
}

void *lockless_ght_next_keysize(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **pp_key, unsigned int *size) {
	return lockless_next_keysize(p_ht, p_iterator, pp_key, size);
}

/*
 * this function remove the entry which iterator is pointing to it.
 */
void *lockless_ght_iterator_remove(ght_hash_table_t *p_ht, lockless_ght_iterator_t *p_iterator, const void **p_key) {

 	ght_hash_entry_t *p_del = NULL;
 	ght_uint32_t l_key;
 	void *p_ret = NULL;

 	ght_hash_entry_t *p_unext = NULL;
 	ght_hash_entry_t *p_uprev = NULL;

 	assert(p_ht);

 	//hk_fill(&key, i_key_size, p_key_data);
 	l_key = get_hash_value(p_ht, &p_iterator->p_entry->key) & p_ht->i_size_mask;
 	/* Check that the first element really is the first */
 	assert((p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1));
	
 	Force_Mark_Delete( &(p_iterator->p_entry->p_next) );
 	EVENTS('z', p_iterator->p_entry);

 	p_del = p_iterator->p_entry;

 	p_iterator->was_forwarded_by_delete = 'n';
 	lockless_ght_next(p_ht, p_iterator, p_key);
 	p_iterator->was_forwarded_by_delete = 'y';
 	EVENTS('y', p_del);
 	
 	fail_iterator_remove:
 	if (p_del && p_del->p_data != NULL ) {

		p_unext = p_del->p_next;
 		UnMark( &p_unext );

 		while( !Mark_delete( &(p_del->p_prev) ) );
 		p_uprev = p_del->p_prev;
 		UnMark( &p_uprev );
 		EVENTS('x', p_del);
 		if (p_uprev != NULL) {
 			EVENTS('w', p_del);
 			if (!CAS1(&(p_uprev->p_next), &p_del, &p_unext)) {
 				EVENTS('v', p_del);
 				while(!UnMark( &(p_del->p_prev) ));
 				goto fail_iterator_remove;
 			}
 		}
 		else {
 			EVENTS('u', p_del);
 			p_del->p_newer = p_ht->pp_entries[l_key];

 			if (!CAS1(&(p_ht->pp_entries[l_key]), &p_del, &p_unext)) {
 				EVENTS('t', p_del);
 				while( !UnMark( &(p_del->p_prev) ) );
 				goto fail_iterator_remove;
 			}
 		}

 		fail_nxt_iter:
 		if (p_unext != NULL) {
 			EVENTS('s', p_del);
			if (!CAS1(&(p_unext->p_prev), &p_del, &(p_uprev))) {
 				goto fail_nxt_iter;
 			}
 		}

 #if !defined(NDEBUG)
 		p_del->p_next = NULL;
 		p_del->p_prev = NULL;
 #endif /* NDEBUG */
 		FAA(&(p_ht->i_items), -1);

 		FAA(&(p_ht->p_nr[l_key]), -1);
 		p_ret = p_del->p_data;
// 		p_del->p_data = NULL;
// 		p_del->p_prev = 0x1;
// 		p_del->p_next = 0x1;
 		FAA(&(p_del->refCount), 2);
 		EVENTS('T', p_del);
 		he_finalize(p_ht, p_del);

 	}
 	//Unmark_delete( &(p_del->p_next) );
 	//Unmark_delete( &(p_del->p_prev) );
 	return p_ret;
	
	/*ght_hash_entry_t *p_out = p_iterator->p_entry;
	ght_uint32_t l_key;
	void *p_ret = NULL;

	ght_hash_entry_t *p_unext = NULL;
	ght_hash_entry_t *p_uprev = NULL;

	assert(p_ht);

	//hk_fill(&key, i_key_size, p_key_data);
	l_key = get_hash_value(p_ht, &p_iterator->p_entry->key) & p_ht->i_size_mask;
	/* Check that the first element really is the first */
	/*assert((p_ht->pp_entries[l_key]?p_ht->pp_entries[l_key]->p_prev == NULL:1));
	
	p_iterator->was_forwarded_by_delete = 'n';
	lockless_ght_next(p_ht, p_iterator, p_key);
	p_iterator->was_forwarded_by_delete = 'y';

	int alireza =0;
	fail_iterator_remove:
	if (p_out && p_out->p_data != NULL ) {
		if(p_out->p_next == 0x1 && p_out->p_prev == 0x1 ){
			return NULL;
		}
		if (!Mark_delete(&(p_out->p_next)))
			goto fail_iterator_remove;
		if (!Mark_delete(&(p_out->p_prev))){
			while(!UnMark(&(p_out->p_next))){}
			goto fail_iterator_remove;
		}

		p_unext = p_out->p_next;
		UnMark(&p_unext);
		p_uprev = p_out->p_prev;
		UnMark(&p_uprev);

		if (p_uprev != NULL) {
			if (!CAS1(&(p_uprev->p_next), &p_out, &p_unext)) {
				while(!UnMark(&(p_out->p_prev))){}
				while(!UnMark(&(p_out->p_next))){}
				goto fail_iterator_remove;
			}
		} else {
			if (!CAS1(&(p_ht->pp_entries[l_key]), &p_out, &p_unext)) {
				while(!UnMark(&(p_out->p_prev))){}
				while(!UnMark(&(p_out->p_next))){}
				goto fail_iterator_remove;
			}
		}

		fail_nxt: if (p_unext != NULL) {
			if (!CAS1(&(p_unext->p_prev), &p_out, &(p_uprev)))
				goto fail_nxt;
		}

#if !defined(NDEBUG)
		p_out->p_next = NULL;
		p_out->p_prev = NULL;
#endif /* NDEBUG */
		/*FAA(&(p_ht->i_items), -1);

		FAA(&(p_ht->p_nr[l_key]), -1);
		p_ret = p_out->p_data;
		p_out->p_data = NULL;
		p_out->p_next = 0x1;
		p_out->p_prev = 0x1;
		he_finalize(p_ht, p_out);

	}
	return p_ret;*/
}

/* Finalize (free) a hash table */
void ght_finalize(ght_hash_table_t *p_ht) {
	int i;

	assert(p_ht);

	if (p_ht->pp_entries) {
		/* For each bucket, free all entries */
		for (i = 0; i < p_ht->i_size; i++) {
			free_entry_chain(p_ht, p_ht->pp_entries[i]);
			p_ht->pp_entries[i] = NULL;
		}
		free(p_ht->pp_entries);
		p_ht->pp_entries = NULL;
	}
	if (p_ht->p_nr) {
		free(p_ht->p_nr);
		p_ht->p_nr = NULL;
	}

	free(p_ht);
}

/* Rehash the hash table (i.e. change its size and reinsert all
 * items). This operation is slow and should not be used frequently.
 */
void ght_rehash(ght_hash_table_t *p_ht, unsigned int i_size) {
	ght_hash_table_t *p_tmp;
	ght_iterator_t iterator;
	const void *p_key;
	void *p;
	int i;

	assert(p_ht);

	/* Recreate the hash table with the new size */
	p_tmp = ght_create(i_size);
	assert(p_tmp);

	/* Set the flags for the new hash table */
	ght_set_hash(p_tmp, p_ht->fn_hash);
	ght_set_alloc(p_tmp, p_ht->fn_alloc, p_ht->fn_free);
	ght_set_heuristics(p_tmp, GHT_HEURISTICS_NONE);
	ght_set_rehash(p_tmp, FALSE);

	/* Walk through all elements in the table and insert them into the temporary one. */
	for (p = ght_first(p_ht, &iterator, &p_key); p; p = ght_next(p_ht, &iterator, &p_key)) {
		assert(iterator.p_entry);

		/* Insert the entry into the new table */
		if (ght_insert(p_tmp, iterator.p_entry->p_data, iterator.p_entry->key.i_size, iterator.p_entry->key.p_key) < 0) {
			fprintf(stderr, "hash_table.c ERROR: Out of memory error or entry already in hash table\n"
					"when rehashing (internal error)\n");
		}
	}

	/* Remove the old table... */
	for (i = 0; i < p_ht->i_size; i++) {
		if (p_ht->pp_entries[i]) {
			/* Delete the entries in the bucket */
			free_entry_chain(p_ht, p_ht->pp_entries[i]);
			p_ht->pp_entries[i] = NULL;
		}
	}

	free(p_ht->pp_entries);
	free(p_ht->p_nr);

	/* ... and replace it with the new */
	p_ht->i_size = p_tmp->i_size;
	p_ht->i_size_mask = p_tmp->i_size_mask;
	p_ht->i_items = p_tmp->i_items;
	p_ht->pp_entries = p_tmp->pp_entries;
	p_ht->p_nr = p_tmp->p_nr;

	p_ht->p_oldest = p_tmp->p_oldest;
	p_ht->p_newest = p_tmp->p_newest;

	/* Clean up */
	p_tmp->pp_entries = NULL;
	p_tmp->p_nr = NULL;
	free(p_tmp);
}

int __attribute__((noinline)) CAS(uint64_t *addr, uint64_t old, uint64_t new) {
	asm volatile goto (
			"movq %[old], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movq %[new], %%RCX\n\t"
			"movl %%ECX, %%EBX\n\t"
			"shr $32, %%RCX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[done]"
			:
			:[old] "m" (old), [new] "m" (new), [addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:done
	);
	return 0;
	done: return 1;
}

int __attribute__((noinline)) CAS1(ght_hash_entry_t **addr, ght_hash_entry_t **old, ght_hash_entry_t **new) {
	asm volatile goto (
			"movq %[old], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movq %[new], %%RCX\n\t"
			"movl %%ECX, %%EBX\n\t"
			"shr $32, %%RCX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[done]"
			:
			:[old] "m" (*old), [new] "m" (*new), [addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:done
	);
	return 0;
	done: return 1;
}

/**this CAS is for when our old value is not a valid address
 * and it have 0x0 value.
 */
int __attribute__((noinline)) CAS2(ght_hash_entry_t **addr, ght_hash_entry_t *old, ght_hash_entry_t **new) {
	asm volatile goto (
			"movq %[old], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movq %[new], %%RCX\n\t"
			"movl %%ECX, %%EBX\n\t"
			"shr $32, %%RCX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[done]"
			:
			:[old] "m" (old), [new] "m" (*new), [addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:done
	);
	return 0;
	done: return 1;
}
/**
 * this function set the lowest 2 bits of memory address to zero
 * and return true if the memory location was not changed. otherwise
 * return false and don't change the memory address. In other words,
 * this function remove both marks of deletetion and iteration.
 *
 * @param addr the address of memory location which must be UnMark!
 *
 * @return a boolean value indicating the successfulness of operation.
 */
int __attribute__((noinline)) UnMark(ght_hash_entry_t **addr) {
	asm volatile goto(
			"movq %[addr], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movl %%EAX, %%EBX\n\t"
			"movl %%EDX, %%ECX\n\t"
			"andl $0xfffffffc, %%EBX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[success]"
			:
			:[addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:success
	);
	return 0;
	success: return 1;
}

/**
 * this function just delete the iteration mark.
 */
int __attribute__((noinline)) UnMark_iteration(ght_hash_entry_t **addr) {
	asm volatile goto(
			"movq %[addr], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movl %%EAX, %%EBX\n\t"
			"movl %%EDX, %%ECX\n\t"
			"andl $0xfffffffd, %%EBX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[success]"
			:
			:[addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:success
	);
	return 0;
	success: return 1;
}

/**
 * this function add the value to the address in an atomic manner.
 * the value can be a negative number.
 */
inline void FAA(unsigned int *address, signed int value) {
	asm volatile (
			"movl %[val], %%eax\n\t"
			"lock\n\t"
			"XADD %%eax, %[addr]"
			:
			:[val] "m" (value), [addr] "m" (*address)
			:"eax", "memory"
	);
}

/**
 * this function Set the lowest bit of 'addr' argument if it wasn't
 * mark before and return true. if the argument has been marked, don't
 * change it and return false.
 *
 * @param addr the address of memory location that must be mark!
 *
 * @return a boolean value indicating the successfulness of operation.
 */
int __attribute__((noinline)) Mark_delete(ght_hash_entry_t **addr) {
	asm volatile goto (
			"movq %[addr], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movl %%EDX, %%ECX\n\t"
			"movl %%EAX, %%EBX\n\t"
			"andl $0xfffffff8, %%EAX\n\t"
			"orl $0x1, %%EBX\n\t"
			"andl $0xfffffffd, %%EBX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[success]"
			:
			:[addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:success
	);
	return 0;
	success: return 1;
}

int __attribute__((noinline)) Unmark_delete(ght_hash_entry_t *addr) {
	asm volatile goto(
			"movq %[addr], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movl %%EAX, %%EBX\n\t"
			"movl %%EDX, %%ECX\n\t"
			"andl $0xfffffffe, %%EBX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[success]"
			:
			:[addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:success
	);
	return 0;
	success: return 1;
}

int __attribute__((noinline)) Force_Mark_Delete(ght_hash_entry_t **addr) {
	asm volatile goto (
		"movq %[addr], %%RDX\n\t"
		"movl %%EDX, %%EAX\n\t"
		"shr $32, %%RDX\n\t"
		"movl %%EDX, %%ECX\n\t"
		"movl %%EAX, %%EBX\n\t"
		"andl $0xfffffffa, %%EAX\n\t"
		"orl $0x1, %%EBX\n\t"
		"lock\n\t"
		"cmpxchg8b %[addr]\n\t"
		"jz %l[success]"
		:
		:[addr] "m" (*addr)
		:"rax", "rbx", "rcx", "rdx"
		:success
		);
	return 0;
	success: return 1;
}

/**
 *this function mark the second bit of address if and only if
 *the address have no mark.
 */
int __attribute__((noinline)) Mark_iteration(ght_hash_entry_t **addr) {
	asm volatile goto (
			"movq %[addr], %%RDX\n\t"
			"movl %%EDX, %%EAX\n\t"
			"shr $32, %%RDX\n\t"
			"movl %%EAX, %%EBX\n\t"
			"movl %%EDX, %%ECX\n\t"
			"orl $0x2, %%EBX\n\t"
			"andl $0xfffffffe, %%EBX\n\t"
			"andl $0xfffffff8, %%EAX\n\t"
			"lock\n\t"
			"cmpxchg8b %[addr]\n\t"
			"jz %l[success]"
			:
			:[addr] "m" (*addr)
			:"rax", "rbx", "rcx", "rdx"
			:success
	);
	return 0;
	success: return 1;
}

int __attribute__((noinline)) Has_Delete_Mark(ght_hash_entry_t **addr) {
	asm volatile goto (
			"movq %[addr], %%RDX\n\t"
			"andl $0x1, %%EDX\n\t"
			"test %%EDX, %%EDX\n\t"
			"jz %l[fail]"
			:
			:[addr] "m" (*addr)
			:"rdx"
			:fail
	);
	return 1;
	fail: return 0;
}

int __attribute__((noinline)) Has_Iteration_Mark(ght_hash_entry_t **addr) {
	asm volatile goto (
			"movq %[addr], %%RDX\n\t"
			"andl $0x2, %%EDX\n\t"
			"test %%EDX, %%EDX\n\t"
			"jz %l[fail]"
			:
			:[addr] "m" (*addr)
			:"rdx"
			:fail
	);
	return 1;
	fail: return 0;
}

int __attribute__((noinline)) Has_Mark(ght_hash_entry_t **addr) {
	asm volatile goto (
			"movq %[addr], %%RDX\n\t"
			"andl $0x03, %%EDX\n\t"
			"test %%EDX, %%EDX\n\t"
			"jz %l[fail]"
			:
			:[addr] "m" (*addr)
			:"rdx"
			:fail
	);
	return 1;
	fail: return 0;
}

int __attribute__((noinline)) Release(uint64_t *addr) {
	fail:
	asm volatile goto (
				"movq %[addr], %%RDX\n\t"
				"movl %%EDX, %%EAX\n\t"
				"movq %%RDX, %%RCX\n\t"
				"shr $32, %%RDX\n\t"
				"cmp $1, %%RCX\n\t"
				"je L3\n\t"
				"cmp $0, %%RCX\n\t"
				"je L1\n\t"
				"sub $2, %%RCX\n\t"
				"cmp $0, %%RCX\n\t"
				"jne L2\n\t"
				"L1: \n\t"
				"movq $1, %%RCX\n\t"
				"L2: \n\t"
				"movl %%ECX, %%EBX\n\t"
				"shr $32, %%RCX\n\t"
				"cmpxchg8b %[addr]\n\t"
				"jnz %l[fail]\n\t"
				"jmp %l[success]\n\t"
				"L3:\n\t"
				:
				:[addr] "m" (*addr)
				:"rax", "rbx", "rcx", "rdx"
				:fail, success
		);
	return 0;
	success: return 1;
}

//this function operate in two different states
//1. if the data of input address is even, then return 0;
//2. if the data of input address is odd , then plus 2 and return 1;
int __attribute__((noinline)) safeRead(uint64_t *addr) {
	fail:
	asm volatile goto (
				"movq %[addr], %%RDX\n\t"
				"movl %%EDX, %%EAX\n\t"
				"movq %%RDX, %%RCX\n\t"
				"shr $32, %%RDX\n\t"
				"add $2, %%RCX\n\t"
				"movq %%RCX, %%RBX\n\t"
				"andl $0x1, %%EBX\n\t"
				"test %%EBX, %%EBX\n\t"
				"jnz finish\n\t"
				"movl %%ECX, %%EBX\n\t"
				"shr $32, %%RCX\n\t"
				"cmpxchg8b %[addr]\n\t"
				"jnz %l[fail]\n\t"
				"jmp %l[success]\n\t"
				"finish:\n\t"
				:
				:[addr] "m" (*addr)
				:"rax", "rbx", "rcx", "rdx"
				:fail, success
		);
	return 0;
	success: return 1;
}
