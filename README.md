# ASHash

## Requirements
You will need to install the following python libraries to use this code:

- py-radix: https://pypi.python.org/pypi/py-radix
- pybgpstream (from source): https://bgpstream.caida.org/docs/install/pybgpstream
- apsw: https://pypi.python.org/pypi/apsw

## Example
As an example, we look at AS hegemony changes during the Comcast outage caused by Level(3) BGP route leak on Nov. 11th, 2017.

The first step is to fetch corresponding BGP data and compute AS hegemony for this dataset. This may consumes a lot of RAM memory as we need to maintain the RIBs of all analyzed BGP peers. For this example, we employ only one BGP collector, route-views3, that accounts for 17 full feed BGP peers. You'll need about 12GB of free memory to run the following command:

```
python2 src/ashash.py -c route-views3 -o ./Comcast_20171107_rv3/ 2017-11-06T16:00 2017-11-06T22:00
```
The "-c" option designates the BGP collectors that are used. Using more collectors will provide better results but it also consumes more memory. The "-o" option specifies the folder where the results will be saved. And the two dates at the end give the start and end time of the analysis.
This command will take about 20 minutes to fetch data and compute AS hegemony scores. You can watch the progress in the log file (Comcast_20171107_rv3/log_2017-11-06\ 16:00:00.log). All results are stored in a SQLite database (Comcast_20171107_rv3/results_2017-11-06\ 16:00:00.sql).

Then you can visualize the AS hegemony scores for paths towards a certain AS using plotLocalHegemony.py. The following command plots the AS hegemony of transit networks towards one of Comcast ASes, AS33667:
```
python2 src/analysis/plotLocalHegemony.py -i ./Comcast_20171107_rv3/ -a 33667
```
The "-i" option should point to the folder containing results obtained from the previous step, and the "-a" option designates the origin AS of interest.
The plot is available in the ./Comcast_20171107_rv3/ folder, and should look like that:

![Level3 BGP route leak](http://ihr.iijlab.net/static/ihr/AS33667_localHegemony.png)

The bump in Level(3) hegemony (AS3356) at 18:00 UTC reveals the BGP route leak that lasted for about 90 minutes.


## References
[Romain Fontugne, Anant Shah, Emile Aben, The (thin) Bridges of AS Connectivity: Measuring Dependency using AS Hegemony, arXiv:1711.02805.](https://arxiv.org/pdf/1711.02805)
[Romain Fontugne, Anant Shah, Emile Aben, AS Hegemony: A Robust Metric for AS Centrality, SIGCOMM Posters and Demos '17.](http://www.iij-ii.co.jp/en/lab/researchers/romain/papers/romain_sigcomm2017.pdf)
