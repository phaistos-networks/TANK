#pragma once
#include "switch_compiler_aux.h"

struct switch_slist
{
	struct switch_slist *next;
};

#define switch_slist_foreach(PTR, IT) for (I = (PTR)->next; I; I = I->next)

static inline void switch_slist_append(switch_slist *s, switch_slist *a)
{
	a->next = s->next;
	s->next = a;
}

inline void switch_slist_init(switch_slist *l)
{
	l->next = l;
}

static inline switch_slist *switch_slist_removefirst(switch_slist *s)
{
	switch_slist *r = s->next;

	s->next = r->next;
	return r;
}

inline bool switch_slist_isempty(const switch_slist *l)
{
	return l->next == l;
}

inline bool switch_slist_any(const switch_slist *const l)
{
	return l->next != l;
}
	





struct switch_dlist
{
	struct switch_dlist *prev, *next;
};

static inline void switch_dlist_init(switch_dlist *const l)
{
	assume(l);
	l->next = l;
	l->prev = l;
}

// @a to head of the list @d
// e.g LIST.next = a
// i.e d.push_back(a)
static inline void switch_dlist_insert_after(switch_dlist *const d, switch_dlist *const a)
{
        auto *const dn = d->next;

        a->next = dn;
        a->prev = d;
        dn->prev= a;
        d->next = a;
}

static inline void switch_dlist_del(switch_dlist *const d)
{
        auto *const dp = d->prev;
        auto *const dn = d->next;

        dn->prev = dp;
        dp->next = dn;
}

// replace A with B
static inline void switch_dlist_replace(switch_dlist *const a, switch_dlist *const b)
{
	switch_dlist_insert_after(a, b);
	switch_dlist_del(a);
}

// @a to tail of the list @d
// e.g LIST.prev = a
static inline void switch_dlist_insert_before(switch_dlist *const d, switch_dlist *const a)
{
        auto *const dp = d->prev;

        d->prev = a;
        a->next = d;
        a->prev = dp;
        dp->next= a;
}




// switch_dlist_del() and switch_dlist_init() @d
// If you want to e.g move an item in an list elsewhere (e.g LRU move to head), you need 
// to first switch_dlist_del(), then switch_dlist_init() it and THEN switch_dlist_insert_after() it, otherwise, if you only del()it but
// don't initialize it, it will corrupt the list. Use switch_dlist_del_and_reset() always
static inline void switch_dlist_del_and_reset(switch_dlist *const d)
{
        auto *const dp = d->prev;
        auto *const dn = d->next;

	d->next = d;
	d->prev = d;

        dn->prev = dp;
        dp->next = dn;
}


static inline switch_dlist *switch_dlist_poplast(switch_dlist *const d)
{
        auto *const dp = d->prev;

        if (dp == d) 
		return nullptr;
	else
	{
		switch_dlist_del(dp);	
		return dp;
	}
}

static inline switch_dlist *switch_dlist_popfirst(switch_dlist *const d)
{
        auto *const dp = d->next;

        if (dp == d) 
		return nullptr;
	else
	{
		switch_dlist_del(dp);	
		return dp;
	}
}

static inline void switch_dlist_merge(switch_dlist *const d, switch_dlist *const d2)
{
        auto *const dp = d->prev;
        auto *const d2n= d2->next;
        auto *const d2p= d2->prev;

        if (d2n == d2) 
		return; 	// Empty list, don't bother

        dp->next = d2n;
        d2n->prev= dp;
        d->prev  = d2p;
        d2p->next= d;
}

static inline uint32_t switch_dlist_size(const switch_dlist *const l)
{
	uint32_t n = 0;

	for (auto *it = l->next; it != l; it = it->next)
		++n;

        return n;
}

template <typename T>
static inline auto reverseSinglyList(T *h)
{
        T *rh{nullptr};

        while (h)
        {
                auto t = h;

                h = t->next;
                t->next = rh;
                rh = t;
        }
        return rh;
}

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif


// Iterate from head .. to tail
#define switch_dlist_foreach_fromhead(PTR, IT) 	for (IT = (PTR)->next; IT != (PTR); IT = IT->next)
// Iterate from tail .. to head
#define switch_dlist_foreach_fromtail(PTR, IT) 	for (IT = (PTR)->prev; IT != (PTR); IT = IT->prev)

#define switch_dlist_isempty(PTR) 	(((PTR)->next == (PTR)))
#define switch_dlist_any(PTR) 		(((PTR)->next != (PTR)))
// e.g struct container{ ...; struct switch_dlist itemsList; ..};  struct item { ...; struct switch_dlist list; ... }
// struct item *const theItem = switch_list_entry(item, list, container.itemsList.next);
//#define switch_list_entry(T, M, PTR /* switch_dlist ptr */) 	(CONTAINER_OF(PTR, T, M))
#define switch_list_entry(T, M, PTR /* switch_dlist ptr */) 	(containerof(T, M, PTR))

#ifdef __clang__
#pragma GCC diagnostic pop
#endif
