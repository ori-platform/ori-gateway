# Gateway Contract Fixtures

These JSON files are the gateway-side golden fixtures for the runtime/gateway
MQTT and enrichment contracts. Keep them byte-stable: tests unmarshal each file
and marshal it back to the exact same canonical JSON bytes.

SDK alignment: equivalent public fixtures must be mirrored or referenced from
`ori-sdk` when SDK contract coverage is updated. Until that repo owns the public
fixtures, this directory is the canonical gateway fixture source.
