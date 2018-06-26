proto_dist
==========

Distribution protocol for Erlang 21+ fixing the head-of-line blocking.

This prototype has been developed as part of the
[LightKone](https://www.lightkone.eu/) research group.
The goal of LightKone is to develop a scientifically sound and industrially
validated model for doing general-purpose computation on edge networks.

Build
-----

    $ rebar3 compile

Usage
-----

    $ erl -proto_dist proto_tcp

Benchmarking
------------

  On the server:

    $ benchmark_server.sh -p proto_tcp

  On the client:

    $ benchmark_client.sh -p proto_tcp -s $SERVER_HOSTNAME

Using it in Other Projects
--------------------------

This project can be included as a dependency in another project.
It should be included in the release and the option `-proto_dist proto_tcp`
should be added to the EVM. Note that all the node in the cluster must use
this distribution protocol, including remote shells.
