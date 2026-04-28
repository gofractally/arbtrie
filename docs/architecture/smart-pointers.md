# Smart Pointers & Thread Safety

PsiTri uses custom smart pointers (`smart_ptr<T>` and `shared_smart_ptr<T>`) instead of raw pointers because objects can be **relocated** at any time by the background compactor. A smart pointer dereferences through the control block indirection layer, always resolving to the object's current memory address.

This page explains the thread-safety rules, how to share data between threads, and the common patterns.

## The Two Pointer Types

### `smart_ptr<T>` — Thread-Local

`smart_ptr<T>` stores a pointer to the **thread-local** `allocator_session`. It is fast (no atomic lookup on dereference beyond the control block), but it is **bound to the thread that created it**.

```cpp
auto ws   = db->start_write_session();
auto root = ws->get_root(0);   // smart_ptr<alloc_header>
// root is only valid on this thread
```

**Rules:**

- Do **not** pass a `smart_ptr` to another thread. The receiving thread would use the wrong allocator session, corrupting internal state.
- Copy and move are safe **on the same thread**. Copies increment the atomic reference count; moves transfer ownership without touching the ref count.
- When the last `smart_ptr` to an object is destroyed, the object is pushed to a **thread-local release queue** that the background compactor drains asynchronously.

### `shared_smart_ptr<T>` — Cross-Thread Safe

`shared_smart_ptr<T>` stores a `std::shared_ptr<allocator>` instead of a raw session pointer. When you call `.get()`, it dynamically looks up the **current thread's** allocator session, returning a thread-local `smart_ptr` that is safe to use.

```cpp
// Writer thread
auto root = ws->get_root(0);                          // thread-local
sal::shared_smart_ptr<sal::alloc_header> shared(root); // safe to share

// Reader thread
auto local = shared.get();   // resolves to this thread's session
auto rc = cursor(std::move(local));
```

**Rules:**

- The source object **must be read-only** (committed) before wrapping in `shared_smart_ptr`. An assert fires in debug builds if this invariant is violated.
- `shared_smart_ptr` can be freely copied, moved, and destroyed from any thread.
- Each call to `.get()` returns a fresh `smart_ptr` bound to the calling thread's session.

## Reference Counting Internals

Reference counts live in the [control block](control-blocks.md), separate from the object data. The count is a 21-bit field in a 64-bit atomic, supporting up to ~2 million concurrent references.

| Operation | Memory ordering | Why |
|-----------|----------------|-----|
| `retain()` (increment) | `relaxed` | No data dependency — just bookkeeping |
| `release()` when ref > 1 | `relaxed` | Not the last reference, no visibility needed |
| `release()` when ref == 1 | `acquire` | Must see all prior writes before deallocation |
| Object relocation (compactor) | `seq_cst` CAS | Location update must be globally visible |

## Common Patterns

### Pattern 1: Writer Produces, Reader Consumes

The most common pattern. A writer commits data, then shares a snapshot with reader threads.

```cpp
auto db = psitri::database::open("mydb", psitri::open_mode::create_or_open);
auto ws = db->start_write_session();

// Writer populates and commits
auto tx = ws->start_transaction(0);
tx.upsert("key1", "value1");
tx.upsert("key2", "value2");
tx.commit();

// Take a snapshot of the committed root
auto root = ws->get_root(0);
sal::shared_smart_ptr<sal::alloc_header> snapshot(root);

// Spawn reader threads — each gets its own session
std::thread reader([&]() {
    auto rs    = db->start_read_session();
    auto local = snapshot.get();              // thread-local copy
    auto rc    = cursor(std::move(local));
    rc.seek_begin();
    while (!rc.is_end()) {
        // process rc.key() / rc.value()
        rc.next();
    }
});
reader.join();
```

### Pattern 2: Multiple Concurrent Readers

Multiple threads can read the same snapshot simultaneously. Each calls `.get()` to obtain its own thread-local `smart_ptr`.

```cpp
auto root = ws->get_root(0);
sal::shared_smart_ptr<sal::alloc_header> snapshot(root);

std::vector<std::thread> readers;
for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back([&]() {
        auto rs    = db->start_read_session();
        auto local = snapshot.get();          // each thread gets its own copy
        auto rc    = cursor(std::move(local));
        // iterate...
    });
}
for (auto& t : readers)
    t.join();
```

### Pattern 3: Worker Thread Builds a Subtree

A worker thread builds a subtree independently, then passes the result back to the main thread for attachment to the main tree.

```cpp
sal::shared_smart_ptr<sal::alloc_header> subtree;

std::thread worker([&]() {
    auto ws = db->start_write_session();
    // Use a dedicated root to commit, making the tree read-only
    auto tx = ws->start_transaction(1);
    for (int i = 0; i < 1000; ++i)
        tx.upsert("k" + std::to_string(i), "v" + std::to_string(i));
    tx.commit();
    auto root = ws->get_root(1);   // committed = read-only
    subtree = sal::shared_smart_ptr<sal::alloc_header>(root);
});
worker.join();

// Main thread attaches the subtree
auto tx = ws->start_transaction(0);
tx.upsert("worker-result", subtree.get());   // .get() for this thread's session
tx.commit();
```

### Pattern 4: Snapshot Isolation with Concurrent Writes

A snapshot is immutable — the writer can continue modifying the tree without affecting readers holding earlier snapshots.

```cpp
// Take snapshot after first batch
auto root = ws->get_root(0);
sal::shared_smart_ptr<sal::alloc_header> snap_v1(root);

// Writer continues
auto tx = ws->start_transaction(0);
tx.upsert("new-key", "new-value");
tx.remove("key1");
tx.commit();

// Reader on another thread still sees the original data
std::thread reader([&]() {
    auto rs    = db->start_read_session();
    auto local = snap_v1.get();
    // sees the tree as it was before "new-key" was added
});
```

## Thread Safety of `shared_smart_ptr` Itself

`shared_smart_ptr` has the same thread-safety guarantees as `std::shared_ptr`:

- **Different instances** can be read and written concurrently from different threads — safe.
- **The same instance** can be read concurrently from multiple threads — safe.
- **The same instance** must not be written from one thread while being read or written from another — **data race**.

In practice: pass `shared_smart_ptr` **by value** (copy) to each thread, not by reference. Each thread gets its own instance, which is safe.

```cpp
// CORRECT — each thread gets its own copy
auto shared = sal::shared_smart_ptr<sal::alloc_header>(root);
std::thread t1([shared]() { auto local = shared.get(); /* ... */ });
std::thread t2([shared]() { auto local = shared.get(); /* ... */ });

// WRONG — two threads sharing the same instance by reference
// auto& ref = shared;
// std::thread t1([&ref]() { ... });  // data race if anyone writes ref
```

## What NOT to Do

!!! danger "Do not pass `smart_ptr` across threads"
    ```cpp
    // WRONG — undefined behavior
    auto root = ws->get_root(0);
    std::thread t([root]() {    // copies smart_ptr with wrong session
        auto ref = *root;       // uses thread A's session from thread B
    });
    ```

    Use `shared_smart_ptr` instead:
    ```cpp
    // CORRECT
    auto root = ws->get_root(0);
    sal::shared_smart_ptr<sal::alloc_header> shared(root);
    std::thread t([shared]() {
        auto local = shared.get();   // resolves to this thread's session
        auto ref   = *local;
    });
    ```

!!! danger "Do not share uncommitted data"
    ```cpp
    // WRONG — data is not yet read-only
    auto wc = ws->create_write_cursor();
    wc->upsert("key", "value");
    // wc->sync() NOT called — root is still writable
    sal::shared_smart_ptr<sal::alloc_header> shared(wc->root());  // ASSERTS
    ```

    Always commit or sync before sharing:
    ```cpp
    // CORRECT
    wc->sync();
    sal::shared_smart_ptr<sal::alloc_header> shared(wc->root());
    ```

!!! danger "Do not create sessions on one thread and use on another"
    ```cpp
    // WRONG
    auto ws = db->start_write_session();  // on thread A
    std::thread t([ws]() {
        auto tx = ws->start_transaction(0);  // using thread A's session on thread B
    });
    ```

    Each thread must create its own session:
    ```cpp
    // CORRECT
    std::thread t([db]() {
        auto ws = db->start_write_session();  // thread B creates its own
        auto tx = ws->start_transaction(0);
    });
    ```

## Summary

| | `smart_ptr<T>` | `shared_smart_ptr<T>` |
|---|---|---|
| **Session binding** | Bound to creating thread | Dynamically resolves per-thread |
| **Copy across threads** | Undefined behavior | Safe |
| **Performance** | Direct session pointer | One extra indirection on `.get()` |
| **Source requirement** | Any | Must be read-only (committed) |
| **Use case** | All single-thread access | Sharing snapshots between threads |
| **Ref count ops** | Atomic relaxed | Atomic relaxed (same underlying mechanism) |
| **Same-instance thread safety** | Not thread-safe (thread-local by design) | Same as `std::shared_ptr` (read-only sharing OK) |
