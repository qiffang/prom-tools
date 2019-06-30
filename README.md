#prom-tools
The tool based on prometheus.
Currently, it just support export tsdb database directory from prometheus.

#How to use it
## export data
```$xslt
 ./export-data  dump --dump-dir=$dumpdir --min-time=1561701600000 --max-time=1561714950000 $(prometheus data directory) 
```
Start a new prometheus and set data directory as $dumpdir
```$xslt
./prometheus  --config.file=prometheus.yml --storage.tsdb.path=$dumpdir 
```