the objective of this project is to benchmark a set of alternative solutions for
persisting data to disk via a cache from which we can retrieve the data.
We want to measure write speed in both
operations per second(ops)
and bytes per second(bps)
and again for read speeds
for much more data than we can fit in memory
we want to understand and instrument the frequency and size of the various types of data written
(is the same data written very often) and how much time we spend writing each.
similarly we want to understand the frequency and size of data read.
should we keep the most frequently accessed data in memory
should we flush the largest objects to disk
where do we waste most time
what can we optimize
is it worth keeping all keys in memory with a reference of where to find the data.

our benchmark is to generate random data of the sort which we see in our real application
as fast as we possibly can and then to read all of it back again.
when we write it we may write it any number of times with any time interval between writes
similar behaviour for when we read it (there is not a relationship between how many times the data is read and howmany times it is written)
keys will be sha-256 hashes of the data they represent so they will be 'natural' keys and therefore the same data will always have the same key
ie the data will be unique and immutable
in other words one key can only ever represent one data and therefore the data associated with a key will never change

we have the following shape data

model (stochastic|montecarlo|analytic)
trade (BOND|SWAP|FRA|CAP|FLOOR|BONDOPTION|SWAPTION|CONVERTIBLE)
curve (YC|MCAD|DC)
vol (ATM|MIV)
request
scenario
fx
counterparty

A (pseudo-random)generator creates these objects randomly and publishes them onto a queue
from which a set of consumers (cache-writers) puts them into the cache
publishes the keys onto a separate queue
from which another set of consumers (cache-readers) then attempt to read the objects from the cache

All sequences of pseudo-random objects are repeatable
so that the benchmarks are exactly repeatable and comparable

the cache writers record stats for
size, type, duration of each persist operation (and if it was a new cache entry ie a cache hit)
the cache readers record stats for
size, type, duration of each read operation (and if it was a cache hit)

we switch various implementations until we find the one with
highest throughput (can write and then re-read all the data in the least time)
ie sum(size)/sum(duration) * count(type)
with the most deterministic behaviour (no un-predicatable pauses)

we will compare

1) berkeley DB
2) ignite
3) hazelcast
4) coherence
5) ehcache
6) home-brewed file based cache

