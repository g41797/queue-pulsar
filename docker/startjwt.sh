#!/usr/bin/env bash
mkdir tempdir
chmod -c o+w `pwd`/tempdir
ls -al
docker run -itd --privileged --name pulsar -v `pwd`/tempdir:/pulsar/tokens -p 6650:6650 -p 8080:8080 apachepulsar/pulsar bin/pulsar standalone
echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done


docker exec pulsar bin/pulsar tokens create-secret-key --output /pulsar/tokens/jwt.key --base64
docker exec pulsar bin/pulsar tokens create --secret-key file:///pulsar/tokens/jwt.key --subject jobs > `pwd`/tempdir/jwt.token
ls -al tempdir
cat tempdir/jwt.key


docker exec pulsar bin/pulsar-admin namespaces grant-permission public/default --actions produce,consume --role jobs
docker exec pulsar bin/pulsar-admin namespaces permissions public/default

docker cp pulsar:/pulsar/conf/standalone.conf .
sed -i 's/authenticationEnabled=false/authenticationEnabled=true/g' standalone.conf
sed -i 's/authenticationProviders=/authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken/g' standalone.conf
sed -i 's/brokerClientAuthenticationPlugin=/brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken/g' standalone.conf
sed -i 's/brokerClientAuthenticationParameters=/brokerClientAuthenticationParameters=file:\/\/\/pulsar\/tokens\/jwt.token/g' standalone.conf
sed -i 's/tokenSecretKey=/tokenSecretKey=file:\/\/\/pulsar\/tokens\/jwt.key/g' standalone.conf

docker cp standalone.conf pulsar:/pulsar/conf/standalone.conf
docker restart pulsar
echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
echo "-- Pulsar service is ready"

