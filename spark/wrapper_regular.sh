export PYTHONIOENCODING=utf8


spark-submit --packages mysql:mysql-connector-java:5.1.39 build_model_regular.py --driver-memory 5g --executor-memory 10g --spark.driver.maxResultSize 5g  --spark.executor.extraJavaOptions -XX:+UseG1GC --spark.python.worker.memory 5g --spark.shuffle.memoryFraction .5 --packages com.databricks:spark-csv_2.10:1.1.0

ssh -i dev.pem ec2-user@54.209.71.100  << EOF
	source /home/ec2-user/rest_api/venv/bin/activate
	python /home/ec2-user/rest_api/process_data.py
	python /home/ec2-user/rest_api/process_node.py
	curl -H "Content-Type: application/json" -X GET http://localhost:9000/predict/ 
EOF
