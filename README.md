# cpp-near-dedupe
dedupes arrow datasets ( IPC .arrow db's at the moment) using minhash / jaccard similarity scores. Uses multiple threads to speed things up

# warning
this project was thrown together in a few days and fueled by large amounts of coffee, so things are getting cleaned / fixed / improved, and there are likely bugs and other oddities in the code 

# todo
- expose and implement wyas to set max memory usage for some tasks ( like file loader ) and more data to track memory usage across stages ( io in, hashing, comparing, io out )
- change dedupe dupe items to only store the line number, instead of pointer to the dupeItem struct ( RAM savings )
- replace queues with arrays/vectors/ring buffers where possible ( CPU perf, less memory fragmentation )
- better ways to track down thread communication slow downs and thread contention issues
- better readme
- work stealing to speed up jaccard compare checks when other threads are less busy
- better error checking
- handle various arrow formats
- handle different CPU intrinsics for more hardware support
- unit tests
- check for file write permissions on output folder *before* its ready to write out at end of crunching
- clearing output folder on run
- allowing to operate inplace on a dataset
- allow continuing from a partially crunched set of data
- cuda support for even faster fastness

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
THIS IS OUT OF DATE
executing the program with -h or --help will display commandlien arguments
this will be updated shortly...

executing the program with no cmd args will display the expected cmd args.

usage: 
CPPDeduper "\path\to\dirWith\arrowIPCdatasets\inSubfolders" "fileExtensionOfArrowIPCDatasets" "dataColumnName" dupeThreshold "outdir\where\nondupes\are\saved"


NOTE: there is no parameter for hash size or n-gram size. they are compiled in for optimization, and default to n-gram size of 5, and 256 finger print hashes
they can be modified in code on these lines:
static constexpr int HASH_LENGTH_SHINGLES = 5; //words used per hash
static constexpr int NUM_HASHES = 256; //number of hashes for comparison



sample windows commandline:

CPPDeduper "D:\\datasets\\folderWithManySubfolders" ".arrow" "text" 0.7 "d:\\dedupOut"

sample linux:
./CPPDeduper "/mnt/d/datasets/folderWithManySubfolders" ".arrow" "text" 0.7 "/mnt/d/carp/dedupOut"

```


# bugs and issues
- theres very little error handling at the moment, so it can be touchy
- need to manually clear the output folder / make sure its empty / ensure you have permissions, otherwise std::filesystem:: throws an exception and it fails at the end of crunching.
