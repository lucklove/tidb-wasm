This project is deprecated since the [upstream](https://github.com/pingcap/tidb/tree/feature/wasm) will support Wasm in the soon future. At present, you can do this to try it:

```
git clone --single-branch --branch feature/wasm git@github.com:pingcap/tidb.git
cd tidb
make wasm
cd wasm-dist
python -m SimpleHTTPServer 8000
```
