#!/bin/bash

# extracting client credentials
docker run --rm scoriacorp/tls_memcached cat /opt/certs/key/client.key > client.key
docker run --rm scoriacorp/tls_memcached cat /opt/certs/crt/client.crt > client.crt

# extracting CA certificate
docker run --rm scoriacorp/tls_memcached cat /opt/certs/crt/ca-root.crt > ca-root.crt
