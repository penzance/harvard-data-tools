/**
 * This package contains the majority of the code needed to manage leases to
 * ensure mutual exclusion across data pipeline runs. We use leases to protect
 * parts of the code that must be run in isolation from other data pipelines
 * (such as the parts of the identity processing phase that manipulate the
 * shared {@code pii} schema on Redshift).
 * <p>
 * Traditionally, access to a contested resource is performed using some form of
 * locking; a thread attempts to acquire a lock, possibly waiting until that
 * lock is released by another thread, after which it is guaranteed to be the
 * only thread running a critical section of code. Once the thread exits the
 * critical section it releases the lock, allowing another thread to acquire it.
 * <p>
 * In a distributed system this model is insufficient, since the process holding
 * a lock may crash or become inaccessible before releasing the lock. Instead,
 * we use a leasing model, where the lock data structure is extended with a
 * timeout value, after which any other thread is free to acquire the lease. It
 * is the responsibility of the thread that currently holds the lock to renew
 * the lease periodically while it is still in use. As an optimization, a thread
 * can also explicitly release the lock once it has completed its critical
 * section rather than waiting for the lease to expire.
 * <p>
 * Since system clocks will drift between machines, it is important that lease
 * times are designed to be significantly greater than the expected time drift
 * (we assume some form of time synchronization such as NTP is in use). For the
 * identity lease, we acquire the lease for an hour-long period, and try to
 * renew at least once every half hour. Even with this safety margin, it is
 * possible that a system could unexpectedly lose its lease, so the lease
 * renewal step must include a failure mode where the lease is unavailable to
 * renew.
 * <p>
 * The leasing mechanism is implemented on top of DynamoDB, using a table that
 * we only ever access using consistent reads. This way we do not encounter
 * errors due to eventual consistency.
 * <p>
 * A lease object (as implemented in the Lease class) contains three important
 * fields: a string identifying the lease owner (set to the globally-unique run
 * ID in our system), an expiration timestamp, and a version number. We use the
 * version number to eliminate races between reading the current state of a
 * lease and subsequently updating it; DynamoDB allows us to pass an expected
 * version number along with an update operation, giving a form of atomic
 * compare-and-exchange operation.
 * <p>
 * The majority of the lease mangement logic is implemented in the LeaseManger
 * class. See the implementation of that class for a detailed discussion on the
 * algorithm by which leases are acquired and renewed.
 */
package edu.harvard.data.leases;
