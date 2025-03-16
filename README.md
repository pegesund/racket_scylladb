### Scylla driver for Racket

It based on Cassandra code in standard library, but with added support for Scylla protocol (3.4.6)

Reason I created this is because Scylla does not support v3.0 of the protocol

I have probably not followed all protocols, but how to use it is shown in the main.rkt at least

It is also fast and stable, over 20000 reads pr second on my machine, no memory leaks found while running for a while

As I build upon standard library you would need to install this before usage

Usage:

Install scylla library and then:

(require scylla/scylla)

