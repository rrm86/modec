'''
Ronnald Rezende Machado
MODEC

This File aims to:

1. Read parquet files on the silver path in datalake

2. Join and enrich the data

3. Persist the results on the gold path for analysis

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import weekofyear, dayofweek

def create_spark_session():
    '''Create a spark session'''
    spark = SparkSession \
    .builder\
    .master("local") \
    .appName("MODEC") \
    .config("spark.executor.memory", "2gb") \
    .getOrCreate()
    return spark



def read_silver(spark, file_path):
    '''
    Read parquet files
    on the silver path

    Parameters:
    spark : Spark Session
    file_path (str): Path to input data
    '''
    try:
        df_data = spark.read.parquet(file_path)
        print('1->Read {} - OK'.format(file_path))
        return df_data
    except IOError:
        print('read error')

def write_gold(df_out, file_path):
    '''
    Write parquet files
    on the gold path

    Parameters:
    spark : Spark Session
    file_path (str): Path to output data
    '''
    try:
        df_out.write.format('parquet').mode('overwrite').save(file_path)
        print('3--->Write {} - OK'.format(file_path))
    except IOError:
        print('write error')

def process_fact_table(spark):
    '''
    Join and enrich the data

    Parameters:
    spark : Spark Session
    '''
    files_list = ['equipment_sensors',
                  'equipment_failure_sensors',
                  'equipment']

    #dict comprehension
    df_dict = {f : read_silver(spark,
                               'datalake/silver/{0}/'
                               .format(f))
               for f in files_list}


    fact = df_dict.get('equipment_sensors').join(df_dict.get('equipment'),
                                                 ['equipment_id'],
                                                 how='inner')

    fact = fact.join(df_dict.get('equipment_failure_sensors'),
                     ['sensor_id'],
                     how='inner')

    fact = fact.select('*',
                       dayofmonth('date').alias('day'),
                       weekofyear('date').alias('week'),
                       month('date').alias('month'),
                       year('date').alias('year'),
                       dayofweek('date').alias('weekday'))

    print('2-->Enrich - OK ')
    write_gold(fact, 'datalake/gold/fact/')



def main():
    '''
    Main Function
    '''
    spark = create_spark_session()
    process_fact_table(spark)

if __name__ == "__main__":
    main()
