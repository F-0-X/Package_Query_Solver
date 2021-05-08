# 645_project

## Resourse

[PuLR document](https://pypi.org/project/PuLP/)

[paper](http://avid.cs.umass.edu/courses/645/s2021/project/scalable-paql.pdf)

[project discription](http://avid.cs.umass.edu/courses/645/s2021/project/project-paql.pdf)

[click and download TPCH csv dataset](http://avid.cs.umass.edu/courses/645/s2021/project/paql_data/tpch.csv)

[Queries and info](http://avid.cs.umass.edu/courses/645/s2021/project/paql_data/README.txt)

## Instruction
1. Download the tpch.csv and put it in the data/ folder
2. remember to set the working directory to 645_project instead of 645_project/src
3. remember to edit the template for unittest and set the working directory to 645_project

cd 645_project
export PYTHONPATH=$PWD



#### Report result reproduce

1. Download the tpch.csv and put it in the data/ folder

2. Add the repo folder(645_project) to your python path **Very Important**
    ```bash
    $ cd 645_project
    $ export PYTHONPATH=$PWD
    ```

3. Install all the dependency, following is our result for "pip list -V" in our virtual enviorment
    
   Package         Version
   --------------- -------
   amply               0.1.4
   cycler                0.10.0
   docutils            0.17.1
   joblib                1.0.1
   kiwisolver        1.3.1
   matplotlib        3.4.1
   numpy              1.20.2
   pandas              1.2.4
   Pillow                 8.2.0
   pip                      21.1
   PuLP                   2.4
   pyparsing           2.4.7
   python-dateutil 2.8.1
   pytz                     2021.1
   scikit-learn         0.24.2
   scipy                    1.6.3
   setuptools          56.0.0
   six                        1.15.0
   threadpoolctl     2.1.0
   
4. We use xxx to randomly split the origin dataset. If you want to split the file on your own, you can
   run 

   ```bash
   I'm some bash code
   ```
    in this way, your randomly split dataset might be different from us which can lead to slightly different result

    or 

    you can download our split data from google drive

5. To run one query, you need to 

    - Firstly, write corresponding json file and put it in input/ folder. (take a look at the examples in input folder)
      - If you want to run Q1, Q2, Q3 or Q4, we already have them in the input/ folder.  Just make sure you modify the table name so that you can run the query with the dataset you want.

    - Secondly, modify the secret.py in the repo folder if you want to use cplex solver

    - Thirdly, go to the repo folder and run the paql.py

      ```bash
      $ cd 645_project
      $ python3 src/paql.py -ace --input_file Q2.json --num_groups 600
      ```

      \-a                                  Turn on the advance mode, choose to use Sketch-Refine. Otherwise, we use Direct to solve the query. 

      -c                                  Choose to use cplex ILP solver. Otherwise, we the default solver.

      -e                                 Choose to use the Gaussian partition core (the extension of this project). Otherwise, we use Kmeans partition core.

      --input_file                 Choose the json file corresponding to the query. Otherwise, we choose Q1.json.

      --num_groups           Choose the number of groups you want when partition. By default, we use 400 groups

      > If we used to make partition for current dataset with current partition core and group number, we use the old partition result. Otherwise, we  make partition before querying and store the partition result in temp/ folder.

6. To reproduce our result in our report

   - Firstly, make partition for corresponding query(Take a lot of time and the result will be store in the temp/ folder)

     ```bash
     $ cd 645_project
     $ python3 tests/partition_all.py
     ```

   - Secondly, run all query and record the result (If you don't use cplex, please modify the line 161 in run_more.py. Result will be stored in output/ folder)

     ```bash
     $ cd 645_project
     $ python3 tests/run_more.py
     ```

   - Thirdly, print the figures

     ```bash
     # why write something here
     ```

