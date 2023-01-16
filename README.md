# cpp-near-dedupe
dedupes arrow datasets ( IPC .arrow db's at the moment) using minhash / jaccard similarity scores. Uses multiple threads to speed things up

# warning
this project was thrown together in a few days and feuled by large amounts of coffee, so things are getting cleaned / fixed / improved, and there are likely bugs and other oddities in the code 

# todo
- better readme
- more commandline arguments for more options
- better error checking
- handle various arrow formats
- more perf and ram optimizations
- thread affinities / priority tweaking
- handle different CPU intrinsics for more hardware support
- unit tests
- check for file write permissions on output folder *before* its ready to write out at end of crunching
- clearing output folder on run
- allowing to operate inplace on a dataset
- allow continuing from a partially crunched set of data

# building
- windows

```
visual studio 2022 community edition:
see apache arrow installation docs for installing arrow dependencies

open the sln (for sln based ) 
or 
open the folder with the cmakelists.txt file (for cmake based)
```

- Linux ( tested on ubuntu WSL2 )
```
due to accessing windows mounts, i had to sudo every command to avoid errors.
This may not be neccessary for your particular setup, but better safe than sorry.

Cmake:
see apache arrow isntallation docs for installing arrow dependencies

sudo cmake .
sudo make release
```

# running
- windows/linux

```
executing the program with no cmd args will display the expected cmd args
```

# bugs and issues
- theres very little error handling at the moment, so it can be touchy
- need to manually clear the output folder / make sure its empty / ensure you have permissions, otherwise std::filesystem:: throws an exception and it fails at the end of crunching.
