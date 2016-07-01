#pragma once
#include <atomic>

struct MemoryModel
{
        enum
        {
                // No barriers or synchronization.
                RELAXED = __ATOMIC_RELAXED,
                // Data dependency only for both barrier and synchronization with another thread.
                CONSUME = __ATOMIC_CONSUME,
                // Barrier to hoisting of code and synchronizes with release (or stronger) semantic stores from another thread.
                ACQUIRE = __ATOMIC_ACQUIRE,
                // Barrier to sinking of code and synchronizes with acquire (or stronger) semantic loads from another thread.
                RELEASE = __ATOMIC_RELEASE,
                // Full barrier in both directions and synchronizes with acquire loads and release stores in another thread.
                ACQ_REL = __ATOMIC_ACQ_REL,
                // Full barrier in both directions and synchronizes with acquire loads and release stores in all threads.
                SEQ_CST = __ATOMIC_SEQ_CST,

                // moved out of Barriers NS
                order_relaxed,
                order_acquire,
                order_release,
                order_acq_rel,
                order_seq_cst,

                // memory_order_sync: Forces a full sync:
                // #LoadLoad, #LoadStore, #StoreStore, and most significantly, #StoreLoad
                order_sync = order_seq_cst
        };
};
