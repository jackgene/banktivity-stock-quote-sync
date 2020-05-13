## Build
Pre-requisites:
- CMake - https://cmake.org/download/
- Xcode
```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
```

Command:
```
cmake --build build
```

Output:
```
build/ibdq
```

## Development
```
mkdir xcode
cd xcode
cmake -GXcode ..
```

