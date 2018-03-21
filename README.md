# Nifi Custom ListenTCP Processor

This is a custom processor based on the default `ListenTCP` processor provided by Nifi. In the default `ListenTCP` processor, the delimiter for separating incoming message is hard-coded as `\n` (Note that there is a similar option in the default ListenTCP called `Batching Message Delimiter`, which is the delimiter for concatenating a batch of outgoing messages). In some use cases, it is necessary to configure this delimeter. As a result, I wrote this custom processor, which adds an option for the user to specify the delimiter for incoming message (the delimiter could also be a single character or multiple characters).

## Build Instructions
Build it with Maven:
```
mvn clean install
```
Find the built nar file here:
```
<repo directory>/nifi-custom-listen-tcp-nar/target/nifi-custom-listen-tcp-nar-<version number>.nar
```
and copy it to the following directory of the running Nifi instance:
```
/opt/nifi/nifi-1.4.0/lib
```
Restart Nifi, then you can find the new ``CustomListenTCP`` processor available. You can specify the delimiter for separating incoming messages in the property field `Incoming Message Delimiter`.