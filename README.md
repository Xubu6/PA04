# PA04 

## Team 16 - Alan Xu, Elle Summerfield, Logan Powell

### Milestone 1

Progress Screenshots and code are located in the Git repo at: https://github.com/Xubu6/PA04

### Milestone 2

Screenshots and code are in the Milestone 2 folder of this repo. 

We kept our set-up from Assignment 3, just changing our producer code, to get the energy-data into CouchDB. Once we had the energy data in CouchDB, we installed spark and created the master and worker. We changed the hosts and config files to have the correct private and floating IPs of our Chameleon VMs. Instead of creating clusters, we used the Standalone version of Spark. We then created a file specific to the energy data that would Map and Reduce according to ID of the house, the household, and the plug, the tuple to evaluate. 

Once we had this code, we ran 10 iterations for each variation of M and R workers: M=10 R=2, M=50 R=5, M=100 R=10. With this data, we created plots to figure out the appropriate percentiles. 
