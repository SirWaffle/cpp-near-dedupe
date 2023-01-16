# cpp-near-dedupe
dedupes arrow datasets ( IPC .arrow db's at the moment) using minhash / jaccard similarity scores. Uses multiple threads to speed things up

# warning
this project was thrown together in a few days, so more improvements will come

# todo
- better readme
- more commandline arguments for more options
- better error checking
- handle various arrow formats
- more perf and ram optimizations
- thread affinities / priority tweaking
- handle different CPU intrinsics for more hardware support
- unit tests

# building
- windows

```
visual studio:
open the sln (for sln based ) or the folder with the cmakelists.txt file (for cmake based)
```

- linux ( tested on ubuntu )

```
Cmake:
cmake .
make release
```

# running
- windows/linux

```
executing the program with no cmd args will display the expected cmd args
```
