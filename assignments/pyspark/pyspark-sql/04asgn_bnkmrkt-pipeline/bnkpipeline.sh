#!\bin\bash

echo "Hello Starting the processing"
spark-submit /mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_load.py

if [ $? -eq 0 ]
   then
        echo " data loaded to bnk_parquet_load folder successfully"
        spark-submit /mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_clean.py   
        if [ $? -eq 0 ]
                then
                    echo "data from bnk_parquet_load folder is cleaned and loaded to bnk_parquet_clean folder successfully"
                    spark-submit --jars /home/ram/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar /mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_transform.py
                    if [ $? -eq 0 ]
                        then
                            echo "data is tranformed and loaded to subsciber table in pyspark_training database succesfully"
                            echo "processing completed ...check the pyspark_training database"
                    else
                        echo "error in tranforming  files from bnk_parquet_clean folder in local "
                    fi
        else
            echo "error in cleaning files in bnk_parquet folder in local"
        fi
else
        echo "error in loading files from bankmarket folder in hdfs"

fi