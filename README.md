# ASHash

## Requirements
You will need to install the following python libraries to use this code:

- Python 2.7
- py-radix: https://pypi.python.org/pypi/py-radix
- pybgpstream v1.2: https://github.com/caida/bgpstream
- apsw: https://pypi.python.org/pypi/apsw

## Example
As an example, we look at AS hegemony changes during the Comcast outage caused by Level(3) BGP route leak on Nov. 6th, 2017.

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

## Reading local text files

### File Format
Instead of getting data from BGPStream you can also provide your own BGP data in a text file.
For now the file should have no header and the format should similar to libBGPdump output, that is:
```
BGP version|message time|message type|peer IP address|peer ASN|prefix|AS path|origin|next hop|local preferences|MED|communities| aggregate ID|aggregate IP 
```
Note that actually ashash is using only the fields peer IP address, peer ASN, prefix, and AS path.
The file [data/uniTokyo_20151010.data](data/uniTokyo_20151010.data) shows an example of text file with this format.

### Command Line
To pass a text file to ashash use the option "-f".
For example:
```
python2 src/ashash.py -o ./testTXT/ -f data/uniTokyo_20151010.data 2018-01-01T00:00 2018-01-01T00:00
```

### Limitations
- For now the filters are not implemented for text inputs. You should filter out by yourself undesirable data before executing ashash.
- Similarly timestamps in the file are ignored and the mandatory start and end time from the command line are not used. 
- The input text file is considered as a RIB file. That means ashash reads all messages from the file then compute hegemony only once.

## Online Results
Results from Jan 2004 are available through a REST API on http://ihr.iijlab.net/api/doc. These results are monthly updated, and we are planning to make them hourly available.

### Format
Two formats are available, HTML and JSON. The HTML format allows developpers to easily play with the API. The JSON format provides a programmatic access to our reports. The results are formatted in HTML if you access the API with your web browser, JSON is used otherwise.

### Filtering
You can filter your search by adding parameters in the URL. For example:

- http://ihr.iijlab.net/ihr/api/hegemony/?originasn=20940&timebin__gte=2017-11-01T00:00&timebin__lte=2017-11-20T23:59 provides hegemony scores of transit networks towards Akamai on 2017/11/20.
- http://ihr.iijlab.net/ihr/api/hegemony/?originasn=0&af=4&timebin__gte=2017-11-20T00:00&timebin__lte=2017-11-20T23:59 provides AS hegemony for the IPv4 global graph on 2017/11/20.


## References
- [Romain Fontugne, Anant Shah, Emile Aben, The (thin) Bridges of AS Connectivity: Measuring Dependency using AS Hegemony, arXiv:1711.02805.](https://arxiv.org/pdf/1711.02805)
- [Romain Fontugne, Anant Shah, Emile Aben, AS Hegemony: A Robust Metric for AS Centrality, SIGCOMM Posters and Demos '17.](http://www.iij-ii.co.jp/en/lab/researchers/romain/papers/romain_sigcomm2017.pdf)
