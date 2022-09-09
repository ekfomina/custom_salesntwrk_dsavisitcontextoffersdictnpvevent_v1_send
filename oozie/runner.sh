echo "start_bash"

# Extremely useful option allowing access to Hive Metastore from Spark 2.2
export HADOOP_CONF_DIR=$HADOOP_CONF_DIR
export SPARK_MAJOR_VERSION=$SPARK_MAJOR_VERSION
echo "THE USER_NAME IS $USER_NAME"
if [ "$USER_NAME" == "u_atdisna_s_custom_salesntwrk_int_fasttrack" ] || [ "$USER_NAME" == "u_disna_s_custom_salesntwrk_int_fasttrack" ] || [ "USER_NAME" == "u_itsna_s_custom_salesntwrk_int_fasttrack" ] || [ "USER_NAME" == "u_sna_s_custom_salesntwrk_int_fasttrack" ]; then
  export SPRKRUN="spark2-submit"
  echo "stand is not SDP, using spark2 submit"
  PATH_TO_PYTHON=/opt/cloudera/parcels/PYENV.DML/bin/python3.7
  export HADOOP_CONF_DIR=/etc/hive/conf
else
  export SPRKRUN="spark-submit"
  echo "stand is SDP, using spark submit"
fi

echo "SPARK VERSION IS $SPARK_MAJOR_VERSION"
echo "COMMAND IS $SPRKRUN"
# Fully qualified principal name e.g. USERNAME@REALM
PRINCIPAL=$USER_NAME"@"$REALM
KEYTAB=$USER_NAME".keytab"

set -e # если дочерний процесс упадет, то и родительский (данный) тоже упадет

echo "start get files from hdfs"
hdfs dfs -get "/keytab/${KEYTAB}"
hdfs dfs -get "${PATH_TO_PROJ_HDFS}/scripts"
hdfs dfs -get "${PATH_TO_PROJ_HDFS}/spark_sql_kafka/${SPARK_SQL_KAFKA_FILE_NAME}"
hdfs dfs -get "${PATH_TO_PROJ_HDFS}/spark_sql_kafka/${KAFKA_CLIENT_FILE_NAME}"
hdfs dfs -get "${PATH_TO_PROJ_HDFS}/${JSON_SCHEMA_FILE_NAME}"
hdfs dfs -get "${PATH_TO_PROJ_HDFS}/keystore_jks/${KEYSTORE_JKS_FILE_NAME}"
hdfs dfs -get "${PATH_TO_PROJ_HDFS}/logger.yaml"

echo "arhciving python scripts for spark-submit"
zip -r scripts.zip scripts/ # назвать архив нужно как исходную директорию

set +e # если дочерний процесс (spark-submit) упадет, родительский НЕ упадет

echo "start spark submit"
$SPRKRUN \
  --master yarn \
  --keytab "${KEYTAB}" \
  --queue "${YARN_QUEUE}" \
  --principal "${PRINCIPAL}" \
  --conf spark.yarn.queue="${QUEUE}" \
  --executor-memory "${MEMORY_EXECUTOR}" \
  --num-executors "${NUMBER_EXECUTOR}" \
  --executor-cores "${CORES_EXECUTOR}" \
  --jars "${SPARK_SQL_KAFKA_FILE_NAME}","${KAFKA_CLIENT_FILE_NAME}" \
  --files "${KEYSTORE_JKS_FILE_NAME}","${JSON_SCHEMA_FILE_NAME}",logger.yaml \
  --py-files scripts.zip \
  --conf spark.pyspark.driver.python=$PATH_TO_PYTHON \
  --conf spark.pyspark.python=$PATH_TO_PYTHON \
  --conf spark.sql.catalogImplementation=hive \
  scripts/main.py --logstash_url="${LOGSTASH_URL}" \
  --app_id="${APP_ID}" \
  --kafka_bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --kafka_security_protocol="${KAFKA_SECURITY_PROTOCOL}" \
  --n_partition="${N_PARTITION}" \
  --loading_id="${LOADING_ID}" \
  --increment_mode=$INCREMENT_MODE \
  --point_date_mode=$POINT_DATE_MODE \
  --synthetic_mode=$SYNTHETIC_MODE \
  --archive_mode=$ARCHIVE_MODE \
  --topic_name=$TOPIC_NAME \
  --ctl_url=$CTL_URL \
  --ctl_entity_id=$CTL_ENTITY_ID \
  --json_schema_file_name=$JSON_SCHEMA_FILE_NAME \
  --keystore_jks_file_name=$KEYSTORE_JKS_FILE_NAME \
  --kafka_batch_size=$KAFKA_BATCH_SIZE \
  --kafka_buffer_memory=$KAFKA_BUFFER_MEMORY \
  --kafka_linger_ms=$KAFKA_LINGER_MS \
  --kafka_compression_type=$KAFKA_COMPRESSION_TYPE \
  --kafka_acks=$KAFKA_ACKS \
  --kafka_client_id=$KAFKA_CLIENT_ID

spark_submit_exit_code=$?
echo "spark-submit exit code: ${spark_submit_exit_code}"

# если была ошибка то возвращяем не 0 код возврата
if [[ $spark_submit_exit_code != 0 ]]; then
  exit $spark_submit_exit_code
fi

echo "end_bash"
