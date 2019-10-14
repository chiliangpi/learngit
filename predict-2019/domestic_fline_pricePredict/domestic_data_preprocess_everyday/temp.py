#!/bin/sh
nohup /data/search/envspark/bin/python3 /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_data_preprocess/main.py >> /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/log.txt 2>&1
if [ $? -eq 0 ]
then
    echo "`date +'%Y-%m-%d %T'` : data_preprocess_main.py Succeed ">>log_shell.txt
else
    echo "`date +'%Y-%m-%d %T'` : data_preprocess_main.py Failed ">>log_shell.txt
fi

nohup /data/search/spark_yarn/bin/spark-submit --master yarn \
                                    --conf "spark.pyspark.python=/data/search/envspark/bin/python3" \
                                    --conf "spark.pyspark.driver.python=/data/search/envspark/bin/python3" \
                                    --jars /data/search/spark/jars/bson-3.4.2.jar \
                                    --jars /data/search/spark/jars/mongo-spark-connector_2.11-2.1.1.jar \
                                    --executor-memory 10G \
                                    --driver-memory 1G \
                                    --num-executors 14 \
                                    --deploy-mode client \
                                    --total-executor-cores 4 \
                        /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_data_preprocess/domestic_org_data_first_spark_merge.py >> /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/log.txt 2>&1
if [ $? -eq 0 ]
then
    echo "`date +'%Y-%m-%d %T'` : domestic_org_data_first_spark_merge.py Succeed  ">>log_shell.txt
else
    echo "`date +'%Y-%m-%d %T'` : domestic_org_data_first_spark_merge.py Failed ">>log_shell.txt
fi

nohup /data/search/spark_yarn/bin/spark-submit --master yarn \
                                    --conf "spark.pyspark.python=/data/search/envspark/bin/python3" \
                                    --conf "spark.pyspark.driver.python=/data/search/envspark/bin/python3" \
                                    --jars /data/search/spark/jars/bson-3.4.2.jar \
                                    --jars /data/search/spark/jars/mongo-spark-connector_2.11-2.1.1.jar \
                                    --executor-memory 10G \
                                    --driver-memory 1G \
                                    --num-executors 14 \
                                    --deploy-mode client \
                                    --total-executor-cores 4 \
                        /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_data_preprocess/domestic_features_first_spark_preprocess.py >> /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/log.txt 2>&1
























#!/bin/sh
nohup /data/search/envspark/bin/python3 /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_data_preprocess/main.py
if [ $? -eq 0 ]
then
    echo "`date +'%Y-%m-%d %T'` : data_preprocess_main.py Succeed ">>log_shell.txt
    nohup /data/search/spark_yarn/bin/spark-submit --master yarn \
                                    --conf "spark.pyspark.python=/data/search/envspark/bin/python3" \
                                    --conf "spark.pyspark.driver.python=/data/search/envspark/bin/python3" \
                                    --jars /data/search/spark/jars/bson-3.4.2.jar \
                                    --jars /data/search/spark/jars/mongo-spark-connector_2.11-2.1.1.jar \
                                    --executor-memory 10G \
                                    --driver-memory 1G \
                                    --num-executors 14 \
                                    --deploy-mode client \
                                    --total-executor-cores 4 \
                        /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_data_preprocess/domestic_org_data_first_spark_merge.py
    if [ $? -eq 0 ]
    then
        echo "`date +'%Y-%m-%d %T'` : domestic_org_data_first_spark_merge.py Succeed  ">>log_shell.txt
        nohup /data/search/spark_yarn/bin/spark-submit --master yarn \
                                    --conf "spark.pyspark.python=/data/search/envspark/bin/python3" \
                                    --conf "spark.pyspark.driver.python=/data/search/envspark/bin/python3" \
                                    --jars /data/search/spark/jars/bson-3.4.2.jar \
                                    --jars /data/search/spark/jars/mongo-spark-connector_2.11-2.1.1.jar \
                                    --executor-memory 10G \
                                    --driver-memory 1G \
                                    --num-executors 14 \
                                    --deploy-mode client \
                                    --total-executor-cores 4 \
                        /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_data_preprocess/domestic_features_first_spark_preprocess.py
        if [ $? -eq 0]
        then
            echo "`date +'%Y-%m-%d %T'` : domestic_features_first_spark_preprocess.py Succeed  " >> log_shell.txt
            nohup /data/search/envspark/bin/python3 /data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/script_model/main.py
            if [ $? -eq 0]
            then
                echo "`date +'%Y-%m-%d %T'` : script_model_main.py Succeed">>log_shell.txt
            else
                echo "`date +'%Y-%m-%d %T'` : script_model_main.py Failed">>log_shell.txt
            fi
        else
            echo "`date +'%Y-%m-%d %T'` : domestic_features_first_spark_preprocess.py Failed ">>log_shell.txt
        fi
    else
        echo "`date +'%Y-%m-%d %T'` : domestic_org_data_first_spark_merge.py Failed ">>log_shell.txt
    fi
else
    echo "`date +'%Y-%m-%d %T'` : data_preprocess_main.py Failed ">>log_shell.txt
fi























#!/bin/sh
nohup /data/search/envspark/bin/python3 /data/search/predict-2019/DNN_predict/script_deepFM/script_everyday_run/script_data_preprocess/main.py
if [ $? -eq 0 ]
then
    echo "`date +'%Y-%m-%d %T'` : data_preprocess_main.py Succeed ">>log_shell.txt
    nohup /data/search/spark_yarn/bin/spark-submit --master yarn \
                                        --conf "spark.pyspark.python=/data/search/envspark/bin/python3" \
                                        --conf "spark.pyspark.driver.python=/data/search/envspark/bin/python3" \
                                        --jars /data/search/spark/jars/bson-3.4.2.jar \
                                        --jars /data/search/spark/jars/mongo-spark-connector_2.11-2.1.1.jar \
                                        --executor-memory 10G \
                                        --driver-memory 1G \
                                        --num-executors 14 \
                                        --deploy-mode client \
                                        --total-executor-cores 4 \
                            /data/search/predict-2019/DNN_predict/script_deepFM/script_everyday_run/script_data_preprocess/domestic_featuresProcess_spark_everyDay.py
    if [ $? -eq 0 ]
    then
        echo "`date +'%Y-%m-%d %T'` : domestic_featuresProcess_spark_everyDay.py Succeed ">>log_shell.txt
        nohup /data/search/envspark/bin/python3 /data/search/predict-2019/DNN_predict/script_deepFM/script_everyday_run/script_model/main.py
        if [ $? -eq 0]
        then
            echo "`date +'%Y-%m-%d %T'` : script_model_main.py Succeed">>log_shell.txt
        else
            echo "`date +'%Y-%m-%d %T'` : script_model_main.py Failed">>log_shell.txt
        fi
    else
        echo "`date +'%Y-%m-%d %T'` : domestic_featuresProcess_spark_everyDay.py Failed ">>log_shell.txt
    fi
else
    echo "`date +'%Y-%m-%d %T'` : data_preprocess_main.py Failed ">>log_shell.txt
fi

