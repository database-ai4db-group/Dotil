# About dataset and workload
## WatDiv
We use the official data generator in [WatDiv](https://dsg.uwaterloo.ca/watdiv). We generated four sets of data with scales of 25, 50, 75 and 100 respectively.

For workload, the official offers 20 SPARQLs. We choose 7 star queries(S) and 5 snowflake-shaped queries(F) of them for our workload. We use these queries to construct 8 workloads(S and F for each dataset), in which each query is repeated for from 1 to 5 times respectively.

## LUBM

LUBM dataset is from [LUBM benchmark](http://swat.cse.lehigh.edu/projects/lubm/). There are 1000 `.nt` files about university data. We randomly sample four sets of data with records of 500,000, 1,000,000, 1,500,000 and 2,000,000 respectively.

For workload, the official offers 14 SPARQLs. We use these to construct 4 workloads, in which each query is repeated for from 6 to 10 times respectively. 

# About data generation and collection

1. Create a database on PostgreSQL.
2. Run `insert_data_pg.py`, load the data into the table `t0` in the database.
3. Run `cost_exection.py`. It will construct candidate storages according to the specified dataset and workload. Then it will execute queries on each storage and collect execution time data. The data will be saved in shelve files.
4. Run `model2.py`. It will construct a model according to the specified parameters. And it will train the model with data in the shelve files. At last, it will use cross validation to get the test result.

More details can be found in code comments and our paper. Thank you!