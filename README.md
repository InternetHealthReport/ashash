# ASHash

## Requirements
#### Python libraries:
- py-radix
- simhash
- mmh3
- numpy
- scipy
- matplotlib
- networkx

#### BGP tools:
- bgpdump
- bgpstream

Both bgpdump and bgpstream are optional but you need at least one of them.

## Usage:
#### Example: monitoring the whole Internet from local files:
```
python2 ashash.py /data/myRib.dump /data/myUpdates0.dump /data/myUpdates1.dump
../results/firstTry/
```


#### Example: monitoring Google (AS15169) from local files:
```
python2 ashash.py -f 15169  -N 8 -M 8 /data/myRib.dump /data/myUpdates.dump
../results/google/
```

#### Example: monitoring the whole Internet with BGPstream:
[Let's see what happened on April 22,
2016](http://bgpmon.net/large-hijack-affects-reachability-of-high-traffic-destinations/)
```
python2 ashash.py  "@bgpstream:1461340700,1461340900" "@bgpstream:1461340800,1461346200" ../results/bgpstream/
```

That means we get the RIB files from 16:00 ("@bgpstream:1461340700,1461340900") and update files from 16:00 to 17:30 ("@bgpstream:1461340800,1461346200").


## Output
Results are written to a file in the directory given as last argument (e.g.
../results/firstTry/)
The file is formatted as follows:

``` 
date:time | nb. of announce | nb. of withdraw | median prefix length (announced) | median path length (announced) | nb. origin AS (announced) | nb. peers analyzed (full feed) | total nb. peers seen | nb. ASNs seen | average nbIP/nbPrefix per peer | nb. anomalous sketches | cum. distance | anomalous ASNs 
```
The two important fields are first and the last ones. The first one gives the
beginning of the time bin, and the last one is the list of reported ASNs. 
Each ASN is reported in the form of a 3-tuple *(ASN, nbAnoSketch, centralityChange)*.
- *nbAnoSketch* is the number of sketches where the ASN is found
anomalous. It ranges between 0 and N (default N=16). Low values should be ignored.
- *centralityChange* is the absolute centrality increase/decrease recorded for
  this ASN.

