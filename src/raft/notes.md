## Final Design Decision
1. Use `time.Timer` instead of `time.Sleep`
2. 

## Pain Points
- Because of 1
    - when a follower is just trying to convert to a candidate (before acquiring lock and modify state to a candidate)
        - what if it then hears from a new leader?
    - when a candidate timeout and try again, but before acquiring the lock
        - then knows a new leader?

## Bad decisions tried
- wrongly shared channel among different roles
    - for killing goroutines
    - for checking election timeout