## Design Decisions
- there should be a timeout for kvserver rpc handler
- how does the kvserver know its raft peer is still a leader or not?
- avoid duplicate execution
    - a new RPC implies that the client has seen the reply for its previous RPC
- Let clerk remember which server turned out to be the leader for the last RPC