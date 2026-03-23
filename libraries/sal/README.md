# SAL - Shared Memory Allocatior 

SAL provides a shared memory allocation and managment system with reference counting
smart pointers that can serve as the foundation of many higher-level copy-on-write
persistent data structures that are also "persisted" to disk. 

```
A persistent data structure is one that allows access to any version of the data structure, 
old or new, at any time, rather than modifying the structure in place, which would destroy 
the previous version. In essence, they are designed to be immutable, creating new versions 
instead of modifying existing ones.
```

Objects can be modified in place until a "sync" call is made at which point a snapshot
is taken and all memory used is mprotected to prevent accidental writes. Optionally 
this can corespond with flushing the shared memory to disk. 

Up to 64 different threads can operate and allocate at the same time with almost no
contention because any attempt to modify an object shared by another thread automatically
and transparently produces a copy-on-write version of the object so readers are not
interferred with.

Furthermore, SAL provides a movable memory model, where a background thread can
reogranize where objects are located in memory for optimal compaction and cache locality.

Frequency of Access can be tracked and "hot" items automatically cached in mlock() 
memory and co-located to enable data structures to scale well beyond ram.

Lastly all allocations are linear (advance a pointer) and lock free 99.9% of the time,
making it one of the fastest allocators out there (only needs to lock to grow total memory,
and a background thread tries to keep the buffers full so there is no wait).

## Allocation Hints

One of the more powerful features of SAL is the ability to pass hints to
the allocator so that shared pointers are more likely to share cachelines. This
greatly enhances the performance of tree's and graphs which have many shared 
pointers they need to retain/release when doing copy on wright. 

## Garbage Collection

In the event your program shuts down uncleanly there is a risk that references
to shared objects may be lost causing a "memory leak" in disk storage. SAL provides
a utility to garbage collect and restore proper reference counts. This "stop-the-world"
garbage collection is only required when "the world" has already unexpectedly stopped and
the program was not gracefully shut down. Under normal operations no garbage
collection pauses or leaks occur.

