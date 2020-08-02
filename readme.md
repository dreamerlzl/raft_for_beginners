## Setup
- use relative import instead of setting `GOPATH` as the current dir
- install logrus
```bash
go get "github.com/sirupsen/logrus"
```

## Test
- need python 3.5 or higher
```bash
cd src/raft
python many_test.py [-np num_cores] [-t num_test] [-l time_limit] TestRegex
```
- e.g. `python many_test.py -np 4 -t 1000 -l 3m 2C` means testing 2C 1000 times with 4 cores and time limit 3 minutes.
- logs of tests that failed are stored naming `debug_xx.txt`(xx is the test number)
- use `python many_test.py -h` for help text

## Notes
- time setting: see `raft.go`
    - period for sending heartbeats: 100 ms
    - election timeout: a random number within 250 ~ 500 ms
- For slow CPU(like Intel 4th generation CPU), may need to set larger election timeout 
    - otherwise may fail `one()` because of slow heartbeat broadcasting