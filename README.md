# OrleansEventStoreProvider
An Orleans StreamProvider over EventStore

EventStore is a Event Sourcing provider enabling immutable data streams for applications such as CQRS and other event-sourced applications.

https://geteventstore.com/

## Enabling EventStore in Orleans

In order to enable the use of EventStore in Orleans, we need to build a custom StreamProvider, which this repository provides.

This is a proof of concept repository, and not yet in use in Production.

## Implemented Features
- Can subscribe to an EventStore stream using Orleans StreamNamespace as the EventStore Stream identifier.
- Can subscribe to an EventStore stream, meaning you will receive new events after Subscription.

## Not Implemented Features
- The use of an Orleans StreamGuid has no effect.
- Pushing of events to EventStore through a Subscription (OnNext).
- Reading of streams - meaning you are not currently able to read from the Start or any Position in a Stream.

## Configuration

The actual EventStoreConnection underneath can be configured using the `ConnectionString` property of the Provider Configuration.
The settings available for ConnectionString are documented here: http://docs.geteventstore.com/dotnet-api/3.9.0/connecting-to-a-server/#connection-string

### Example
```
<Provider Type="Orleans.Providers.Streams.EventStore.EventStoreProvider"
          Name="CustomProvider"
          ConnectionString="ConnectTo=tcp://admin:changeit@localhost:1113" />
```
