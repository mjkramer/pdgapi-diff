# pdgapi_diff++

This is a tool for comparing different versions of the SQLite database used by the [PDG API](https://github.com/particledatagroup/api). Changes are presented as insertions, deletions, and updates. For a given table, an attempt is made to identify corresponding rows between the two versions. The best match for a given row is, first, required to have the same `pdgid`; then, for each potential match, a "distance" is computed as the number of unequal columns. The "closest" row then wins. Where this fails to find a unique match, all candidates are presented in the output. When there is no matching row whose distance is below a user-controlled threshold (see below), then the change is presented as an insertion or deletion, rather than an update.

## Usage

The required command-line arguments are the two SQLite files and the name of a table:

```
  pdgapi_diff_pp [OPTION...] db1 db2 table

  -h, --help              Print usage
      --max-dist arg      Maximum distance (default: 3)
      --pedantic          Pedantic mode
      --exclude-cols arg  Columns to exclude (default: "")
```

The `--max-dist` option controls the distance threshold, described above, for finding matching rows. Depending on the nature of the changes between the two versions, tuning this threshold may make the output easier to interpret. If the threshold is too low, updates will be displayed as pairs of insertions/deletions, and if it is too high, ambiguous matches may be presented.

The `--pedantic` option enables a check for uniqueness of matches in both directions. If it finds a unique best-match `row2` in `db2` for a given `row1` in `db1`, but then does not uniquely find `row1` when using `row2` to seach `db1`, a message is printed. The usefulness of this remains to be seen.

The `--exclude-cols` option specifies columns that should be ignored in the comparison (and the output). The `id`, `parent_id`, and `pdigid_id` columns are always excluded. In some cases, excluding the `sort` column is also worthwhile, but this is not done by default.

## Compiling

The build requirements are a recent version of CMake and a compiler with reasonable C++20 support. CMake 3.20 and GCC 13.2 are known to work. All further dependencies are automatically pulled using [vcpkg](https://github.com/microsoft/vcpkg), which does *not* need to be installed systemwide. `vcpkg` will also pull a more recent version of CMake if necessary. The simplest approach is to clone `vcpkg` as a subdirectory of this one:

```bash
git clone https://github.com/mjkramer/pdgapi_diff_pp.git
cd pdgapi_diff_pp
git clone https://github.com/microsoft/vcpkg.git
mkdir build
cmake -D CMAKE_TOOLCHAIN_FILE:FILEPATH=vcpkg/scripts/buildsystems/vcpkg.cmake -B build
cmake --build build
```

This will produce a statically linked binary `build/pdgapi_diff_pp`, which can be run on any machine regardless of what libraries may be installed. During the build, there may be a warning about `dlopen` and static linking. Disregard it.

Note that the `CMakePresets.json` simply provides hints to editors/IDEs (e.g. VS Code), telling them how to invoke CMake. It should not have any effect when manually building from the command line, as above.

### Using a container

The `ubuntu:mantic` image is known to provide a sufficiently recent GCC, e.g.:

``` bash
podman run -it -v $PWD:/pdg ubuntu:mantic /bin/bash
apt update
apt install g++ cmake pkg-config git zip curl
cd /pdg
# compile as above
```

The resulting binary can then be run on the host OS, and the container can be deleted.
