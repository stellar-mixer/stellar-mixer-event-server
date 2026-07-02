# Stellar Mixer Event Server

**Stellar Mixer Event Server** is an infrastructure service for the Stellar Mixer.

It indexes public mixer contract events from Stellar RPC and serves the data that mixer clients need for sync, recovery, and wallet state reconstruction.

This server does not hold user secrets.  
It does not decrypt notes.  
It does not prove anything for the user.  
It only makes public mixer event data easier and cheaper to consume.

## Why this server exists

The Stellar Mixer contract is intentionally minimal.

The contract should enforce only the rules that must live on-chain:

    - verify zero-knowledge proofs
    - move tokens according to valid proofs
    - prevent double spends through nullifiers
    - update mixer state
    - emit enough public events for off-chain infrastructure

The contract should not become a heavy data API.

It should not store user wallet state, serve historical recovery data, maintain per-client sync cursors, or make clients query large amounts of contract history directly from the chain.

That work belongs off-chain.

This server is one of the off-chain infrastructure components of the mixer.

## The problem

A mixer client needs more than the latest contract state.

To show a usable wallet, the client needs to know:

    - which encrypted notes exist
    - which of those notes can be decrypted by the user's mixer identity
    - which nullifiers have already appeared on-chain
    - which locally known notes are already spent
    - where sync stopped last time
    - what new data appeared since then

All of this data is emitted publicly by the mixer contract, but reading it directly from Stellar RPC every time would be inefficient.

Without this server, every client would have to repeatedly:

    - scan large ledger ranges
    - request mixer events from Stellar RPC
    - parse contract event XDR
    - filter relevant mixer events
    - extract encrypted note payloads
    - extract nullifiers
    - rebuild local wallet state from raw chain history

That would push unnecessary repeated load onto RPC nodes.

It would also make recovery slower and less reliable for normal users.

## What this server does

This server indexes the Stellar Mixer contract once and exposes simple append-only sync endpoints.

It stores:

    - encrypted note records
    - note leaf hashes
    - nullifier records
    - source event ids
    - ledger numbers
    - local append indexes
    - indexed state metadata

The important point is that the server stores public or encrypted data only.

Encrypted notes are opaque ciphertexts. The server cannot decrypt them. Only the intended mixer identity can try to decrypt them locally.

Nullifiers are public on-chain spend markers. They are needed so clients can detect which notes are already spent.

## What this server does not do

This server does not:

    - store private keys
    - store recovery phrases
    - decrypt encrypted notes
    - know which notes belong to which user
    - calculate private balances
    - verify user proofs
    - submit transactions
    - replace the mixer contract
    - replace the TreePIR server

It is a sync and recovery indexer for public mixer event data.

## Logical architecture

    Stellar Mixer contract
      emits public events:
        - encrypted notes
        - output leaves
        - nullifiers

    Stellar RPC node
      exposes chain history and event data

    Stellar Mixer Event Server
      indexes events once
      stores compact append-only records
      serves batches to clients

    Mixer client
      downloads encrypted notes and nullifiers
      decrypts only what belongs to its mixer identity
      marks notes as spent using nullifiers
      maintains local wallet state

The server makes the public data easier to consume.  
The client still owns privacy-sensitive interpretation.

## Why not query Stellar RPC directly?

A client can query Stellar RPC directly in theory.

But doing it for every user and every recovery flow is wasteful.

For example, if 10,000 clients all need to recover from the same mixer history, direct RPC scanning means many clients repeat the same expensive work:

    - same ledger ranges
    - same contract event filters
    - same XDR parsing
    - same encrypted note extraction
    - same nullifier extraction

The event server performs that indexing once.

Clients then download already-normalized batches.

This reduces RPC pressure and makes the mixer client simpler, faster, and more reliable.

## Privacy model

Using this server should not break mixer privacy.

The server returns public append-only data:

    - encrypted notes
    - nullifiers
    - event metadata
    - ledger positions

A client does not ask:

    "Give me notes for my identity."

Instead, the client asks:

    "Give me encrypted notes starting from index N."
    "Give me nullifiers starting from index N."

The client downloads batches and tries to decrypt notes locally.

The server does not know which encrypted notes successfully decrypted.  
It does not know which notes belong to the user.  
It does not know the user's mixer identity secret.  
It does not know the user's recovery phrase.

The privacy-sensitive step happens locally on the client.

## Normal sync workflow

When the client already has local state, sync is incremental.

Typical flow:

    1. Client remembers last encrypted note index
    2. Client remembers last nullifier index
    3. Client asks the server for encrypted notes after that index
    4. Client asks the server for nullifiers after that index
    5. Client decrypts new encrypted notes locally
    6. Client updates local note state
    7. Client marks notes spent if their nullifiers appeared

This is cheap.

The client does not need to download all historical data every time. It only continues from the last known sync point.

That is the normal happy path.

## Import and export workflow

A proper full account export can include local wallet metadata, known notes, sync cursors, UI state, and other client-side data.

With a proper export/import, the restored client can continue from where the old client stopped.

That means it only needs to fetch new data:

    - encrypted notes after the previous note cursor
    - nullifiers after the previous nullifier cursor

This is fast and efficient.

In other words, a good backup avoids unnecessary full-history recovery.

## Recovery phrase workflow

A recovery phrase is different.

If the user only has the recovery phrase and no full client export, the client can recreate the mixer identity keys, but it does not know which historical encrypted notes belong to that identity yet.

So the client must scan a much larger range of server data.

Typical recovery flow:

    1. User enters recovery phrase
    2. Client recreates the mixer identity
    3. Client downloads encrypted notes from the event server
    4. Client attempts local decryption of each encrypted note
    5. Notes that decrypt successfully are imported
    6. Client downloads nullifiers
    7. Client marks recovered notes as spent or unspent
    8. Client rebuilds the usable wallet state

This can take time.

That is the tradeoff.

The recovery phrase is enough to recover ownership, but without a full export the client has to search through historical encrypted note data to find what belongs to that identity.

That extra work is the price of not having a complete account export.

## What data is stored

Encrypted note records contain data similar to:

    - append index
    - output leaf hash
    - encrypted note payload
    - source event id
    - ledger number

Nullifier records contain data similar to:

    - append index
    - nullifier value
    - source event id
    - ledger number
    - source operation type

This data is useful because clients need encrypted notes to discover incoming/private outputs, and nullifiers to know whether owned notes have already been spent.

## API

The server exposes:

    GET /health
    GET /ready
    GET /v1/state
    GET /v1/encrypted-notes?index=0
    GET /v1/nullifiers?index=0

The sync endpoints are append-only and cursor-based.

A client can start from index 0 for full recovery, or from a saved index for incremental sync.

Responses include pagination metadata such as:

    - index
    - batch_size
    - end_exclusive
    - next_index
    - total
    - returned
    - has_more
    - items

## Relationship to TreePIR

This server is not the TreePIR server.

The mixer infrastructure has different off-chain roles:

    Stellar Mixer Event Server:
      indexes encrypted notes and nullifiers for sync and recovery

    Stellar Mixer TreePIR Server:
      serves private Merkle path retrieval using TreePIR

The event server helps clients discover and update wallet state.

The TreePIR server helps clients privately fetch Merkle paths.

Both are infrastructure services around the same minimal mixer contract.

## Public infrastructure model

This server can be run by anyone who wants to help support the Stellar Mixer infrastructure.

The data source is public.  
The contract events are public.  
The stored data is either public or encrypted.  
The client verifies and interprets sensitive data locally.

That makes the server suitable for independent operators.

More servers means:

    - less pressure on a single backend
    - less pressure on public RPC nodes
    - better availability
    - better geographic distribution
    - more resilient mixer infrastructure
    - easier client fallback between servers

A mixer should not depend on one official server forever.

The goal is an ecosystem where multiple independent operators can run compatible infrastructure.

## Future x402 incentives

Running an event server costs resources:

    - server hosting
    - disk storage
    - RPC bandwidth
    - indexing time
    - uptime monitoring
    - operational maintenance

At first, this infrastructure can be run voluntarily by the project or community operators.

In future implementations, the mixer infrastructure can add an incentive layer using x402.

The idea is that clients may pay a very small fee for server work:

    1. Client requests sync or recovery data
    2. Server asks for a small x402 payment
    3. Client pays
    4. Server returns the requested batch

This gives independent operators a reason to run public servers.

The fee should be small enough to keep the mixer usable, but enough to make infrastructure operation sustainable.

Over time, this can help the mixer become more decentralized:

    - more people run servers
    - clients can choose between operators
    - infrastructure cost is compensated
    - the system depends less on one maintainer

x402 is not required for the current server to function. It is a future incentive mechanism for scaling public infrastructure.

## Configuration

Default testnet configuration:

    MIXER_ARCHIVE_BIND_ADDR=0.0.0.0:3001
    MIXER_ARCHIVE_STELLAR_RPC_URL=https://soroban-rpc.testnet.stellar.gateway.fm
    MIXER_ARCHIVE_MIXER_CONTRACT_ID=CCVPF5JH57FQV535OYMWWY3VXUC53JEMCUPQIBM6NQKI3FZ5BZ47WVRL
    MIXER_ARCHIVE_START_LEDGER=3388000
    MIXER_ARCHIVE_DB_PATH=./mixer-archive-state.rocksdb
    MIXER_ARCHIVE_POLL_INTERVAL_MS=2000
    MIXER_ARCHIVE_BATCH_LEDGERS=100000
    MIXER_ARCHIVE_EVENTS_LIMIT=100000
    MIXER_ARCHIVE_EVENT_FINALITY_LAG=8
    MIXER_ARCHIVE_CATCHUP_SLEEP_MS=300

The default port is 3001.

For a remote deployment, the server can listen on 3001 internally and be exposed through nginx, Caddy, a load balancer, or a firewall rule.

## Running

Build:

    cargo build

Run:

    cargo run

Test:

    cargo test

Health check:

    curl http://127.0.0.1:3001/health

Ready check:

    curl http://127.0.0.1:3001/ready

State:

    curl http://127.0.0.1:3001/v1/state

Fetch encrypted notes:

    curl "http://127.0.0.1:3001/v1/encrypted-notes?index=0"

Fetch nullifiers:

    curl "http://127.0.0.1:3001/v1/nullifiers?index=0"

## Trust model

Clients should treat this server as a convenience indexer, not as an authority.

A server can be:

    - stale
    - offline
    - incomplete
    - misconfigured
    - slow
    - unavailable

But it should not be able to steal funds or decrypt notes.

The client keeps secrets locally.  
The client decrypts locally.  
The client checks wallet state locally.  
The contract enforces spending rules on-chain.

The server only provides public indexed data.

## Why it matters

The mixer contract stays small.

RPC nodes avoid repeated heavy scanning from every client.

Clients get faster sync and practical recovery.

Users can recover from a phrase even if they lost local state, although full recovery may require scanning a lot of historical encrypted data.

Independent operators can run infrastructure.

Future x402 payments can make that infrastructure economically sustainable.

That is the role of the Stellar Mixer Event Server.
