# nifi-experiments
Controllers, Processors and other Experiments on Nifi


### Experiment 1 : 

##### Externalizing Properties and using across Flows

Option 1 : ControllerService that reads and sets values into DistributedMapCache.  Using fetchdistributedmapcache component to read from the cache and set as attribute just before use.

Option 2 : ControllerService + Processor that set all config properties as attributes
