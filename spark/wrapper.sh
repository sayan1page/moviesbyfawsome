export PYTHONIOENCODING=utf8

spark-submit --packages mysql:mysql-connector-java:5.1.39 build_model_first.py --driver-memory 5g --executor-memory 10g --spark.driver.maxResultSize 5g  --spark.executor.extraJavaOptions -XX:+UseG1GC --spark.python.worker.memory 5g --spark.shuffle.memoryFraction .5 --packages com.databricks:spark-csv_2.10:1.1.0


ssh -i dev.pem ec2-user@54.209.71.100 << EOF
	source /home/ec2-user/rest_api/venv/bin/activate
	python /home/ec2-user/rest_api/process_data_first.py
	nohup uwsgi --http :9000  --wsgi-file /home/ec2-user/rest_api/predictor_service.py --callable api &>/dev/null &disown
EOF
