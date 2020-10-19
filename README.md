# prom-tools
The tool based on prometheus.
Currently, there are 2 tools in the repo. One is used to import data to prometheus and the other is used to export data from prometheus.

# Import Tool
## How to use it
### import data
```$xslt
  ./import-tool import --input=$SimpleData --output=$PrometheusDataDirectory
```
#### SimpleData Format
```$xslt
{"__name__":"oiwwpnxbzvnglxqfmmgydouluripxyalq","blppopdupk":"cnzzbfczfyogugkqbbgptameitukmyqrvfdnbvuennkrjroklunnmhonozwjbhtcyxtmrtslabqlkoimdafoipcrdbtjaxlzlebaiwkjzzpuusp","bvqcfmtc":"nrmpn","etunlkkq":"jlc","ieh":"nrcqguxwfdarfbnnjwrqyavsvr","igaxksxlcgqesc":"ymmoqcbydfyiiqjarxdplpejidikup","peyxeulfptstx":"mznnnpqbwkjjh","pwtdcjrs":"kupicpeeswkcvcqjsbntrqjrzqceppkgkkglgbckqrwo","vgcdywyzlg":"ucafvj","xlqhwhxrcya":"ztnhtzzrz","xtbla":"mznnnpqbwkjjh","zigoeqifdui":"mnjbteqhtkxeovesczl","zxknjgnlwexn":"hcasvfr", "__value__":"111", "__time__":1585799158000}
```
I extend the format of prometheus label, there are 2 special labels: `__time__` and `__value__`.

`__time__`:  the timestamp of the series.

`__value`: the value of the series.

They are not appended the collection of prometheus labels.

# Export Tool
## How to use it
### export data
```$xslt
 ./export-data  dump --dump-dir=$dumpdir --min-time=1561701600000 --max-time=1561714950000 $(prometheus data directory) 
```
Start a new prometheus and set data directory as $dumpdir
```$xslt
./prometheus  --config.file=prometheus.yml --storage.tsdb.path=$dumpdir 
```