# Rinds
A collection of feeds under one roof.

I don't like Docker and I don't need to compile this, okay thanks!

## Running
aint that hard, go install go, setup postgres, and generate the required feed definitions under your user account. you can use https://pdsls.dev to generate a `app.bsky.feed.generator` record.
Set the `rkey` to the desired short url value. itll look like: 
```
https://bsky.app/profile/{you}/feed/{rkey}
```
for the contents you can use the example below
```
{
  "did": "did:web:${INSERT DID:WEB HERE}",
  "$type": "app.bsky.feed.generator",
  "createdAt": "2025-01-21T11:33:02.396Z",
  "description": "wowww very descriptive",
  "displayName": "Cool Feed Name",
}
```

## Env
You can check out `.env.example` for an example


## Postgres
Be sure to set up `.env` correctly

All relevant tables should be created automatically when needed.

## Index
You should start Postgres first
Then go run the firehose ingester in
```
cd ./indexer
```
and go compile it
```
go build -o indexer ./indexer.go && export $(grep -v '^#' ./../.env | xargs) && ./indexer
```
after it has been compiled, you can use `rerun.sh` to ensure it will automatically recover after failure

## Serve
Make sure the indexer (or at least Postgres) is running first:
```
go build -o feedgen cmd/main.go && export $(grep -v '^#' ./.env | xargs) && ./feedgen
```
the logs are pretty verbose imo, fyi

## Todo
- [ ] Faster Indexing
- [x] Proper Up-to-Date Following Indexing
- [x] Repost Indicators
- [x] Cache Timeouts
  - [x] Likes
  - [x] Posts
  - [x] Feed Caches
- [ ] More Fresh Feed Variants
  - [ ] unFresh
  - [x] +9 hrs
  - [ ] Glimpse
  - [ ] Media
    - [ ] Fresh: Gram
    - [ ] Fresh: Tube
    - [ ] Fresh: Media Only
    - [ ] Fresh: Text Only

## Architecture
Based on [go-bsky-feed-generator](https://github.com/ericvolp12/go-bsky-feed-generator). Read the README in the linked repo for more info about how it all works.

### /feeds/static
Basic example feed from the template. Kept as a sanity check if all else seems to fail.

### /feeds/fresh
Fresh feeds, all based around a shared Following feed builder and logic to set posts as viewed. May contain some remnant old references to the old name "rinds".

