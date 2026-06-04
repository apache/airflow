Deprecated elements from ``ingress`` have been removed.

* ``ingress.enabled`` has been replaced. There are now separate flags to control the API server and
   flower individually, ``ingress.web.enabled`` and ``ingress.flower.enabled``.
* ``ingress.apiServer.host`` has been renamed to ``ingress.apiServer.hosts`` and is now an array.
* ``ingress.apiServer.tls`` has been renamed to ``ingress.apiServer.hosts[*].tls`` and can be set per host.
* ``ingress.web.host`` has been renamed to ``ingress.web.hosts``
* ``ingress.web.tls`` has been renamed to ``ingress.web.hosts[*].tls``
* ``ingress.flower.host`` has been renamed to ``ingress.flower.hosts``
* ``ingress.flower.tls`` has been renamed to ``ingress.flower.hosts[*].tls``
* ``ingress.statsd.host`` has been renamed to ``ingress.statsd.hosts`` and is now an array.
* ``ingress.pgbouncer.host`` has been renamed to ``ingress.pgbouncer.hosts`` and is now an array.
