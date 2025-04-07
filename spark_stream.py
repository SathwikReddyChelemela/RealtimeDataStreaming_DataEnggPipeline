import logging
import subprocess
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users(
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                   "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                   "org.apache.kafka:kafka-clients:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config('spark.executor.memory', '1g') \
            .config('spark.driver.memory', '1g') \
            .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint') \
            .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
            .config('spark.sql.catalog.lh', 'com.datastax.spark.connector.datasource.CassandraCatalog') \
            .config('spark.jars.ivy', '/tmp/.ivy') \
            .config('spark.driver.host', 'localhost') \
            .config('spark.driver.bindAddress', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    try:
        logging.info("Attempting to connect to Kafka...")
        logging.info("Kafka bootstrap servers: localhost:9092")
        logging.info("Subscribing to topic: users_created")
        
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {str(e)}")
        logging.error("Please check if Kafka is running and the topic 'users_created' exists")
        raise  # Re-raise the exception instead of returning None


def create_cassandra_connection():
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempting to connect to Cassandra (attempt {attempt + 1}/{max_retries})...")
            # connecting to the cassandra cluster
            cluster = Cluster(['localhost'], port=9042)
            session = cluster.connect()
            logging.info("Successfully connected to Cassandra")
            return session
        except Exception as e:
            logging.error(f"Could not create cassandra connection on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Could not connect to Cassandra.")
                return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    try:
        # Debug: Print the raw Kafka data schema
        logging.info("Raw Kafka data schema:")
        logging.info(spark_df.printSchema())
        
        # Process the streaming data
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
        # Debug: Print the processed schema
        logging.info("Processed data schema:")
        logging.info(sel.printSchema())
        
        return sel
    except Exception as e:
        logging.error(f"Error processing Kafka data: {str(e)}")
        raise


def ensure_kafka_topic_exists():
    """Ensure the Kafka topic exists, create it if it doesn't."""
    try:
        logging.info("Checking if Kafka topic 'users_created' exists...")
        
        # Run kafka-topics.sh to list topics
        cmd = "docker exec broker kafka-topics --list --bootstrap-server localhost:29092"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if "users_created" not in result.stdout:
            logging.info("Topic 'users_created' does not exist. Creating it...")
            
            # Create the topic
            cmd = "docker exec broker kafka-topics --create --topic users_created --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logging.info("Topic 'users_created' created successfully")
            else:
                logging.error(f"Failed to create topic: {result.stderr}")
        else:
            logging.info("Topic 'users_created' already exists")
            
    except Exception as e:
        logging.error(f"Error checking/creating Kafka topic: {str(e)}")


def produce_sample_data():
    """Produce sample data to the Kafka topic."""
    try:
        logging.info("Producing sample data to Kafka topic 'users_created'...")
        
        # Sample JSON data
        sample_data = '''
        {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "first_name": "John",
            "last_name": "Doe",
            "gender": "male",
            "address": "123 Main St",
            "post_code": "12345",
            "email": "john.doe@example.com",
            "username": "johndoe",
            "dob": "1990-01-01",
            "registered_date": "2023-01-01",
            "phone": "123-456-7890",
            "picture": "https://example.com/picture.jpg"
        }
        '''
        
        # Write the sample data to a temporary file
        with open("/tmp/sample_data.json", "w") as f:
            f.write(sample_data)
        
        # Use kafka-console-producer to send the data
        cmd = "docker exec -i broker kafka-console-producer --topic users_created --bootstrap-server localhost:29092 < /tmp/sample_data.json"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info("Sample data produced successfully")
        else:
            logging.error(f"Failed to produce sample data: {result.stderr}")
            
    except Exception as e:
        logging.error(f"Error producing sample data: {str(e)}")


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()