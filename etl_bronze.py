'''
Ronnald Rezende Machado
MODEC

This File aims to:

1. Read a text file

2. Format and clean data

3. Write data on the silver path
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace



def create_spark_session():
    '''Create a spark session'''
    spark = SparkSession \
    .builder\
    .master("local") \
    .appName("MODEC") \
    .config("spark.executor.memory", "2gb") \
    .getOrCreate()
    return spark


def process_equipment_failure_sensors(spark, input_data, output_data):
    '''
    Write parquet files
    on the gold path

    Parameters:
    spark : Spark Session
    input_data (str): Path to input data
    output_data (str): Path to output data
    
    '''
    try:
        df_data = spark.read.format('csv').option("sep", '\t').load(input_data)
        print('1->Read {} - OK'.format(input_data))
    except IOError:
        print('read error')
    
    df_data = df_data.withColumn('date',
                                 regexp_extract('_c0', r'(\d+-\d+-\d+\s\d+:\d+:\d+)', 1)
                                 .alias('Date').cast('timestamp')
                                 ).drop('_c0')

    df_data = df_data.withColumn('error',
                                 when(df_data._c1 == 'ERROR', 1).otherwise(0)
                                 ).drop('_c1')

    df_data = df_data.withColumn('sensor_id',
                                 regexp_replace('_c2', '(\D)', '').cast('integer')
                                 ).drop('_c2')

    df_data = df_data.drop('_c3')

    df_data = df_data.withColumn('temperature',
                                 regexp_extract('_c4', r'(\d+.\d+)', 1).cast('float')
                                ).drop('_c4')

    df_data = df_data.withColumn('vibration',
                                 regexp_extract('_c5', r'([\-\+]?\d+.\d+)', 1).cast('float')
                                 ).drop('_c5')

    print('2--->Format and clean data {} - OK'.format(input_data))

    try:
        df_data.write.format('parquet').mode('overwrite').save(output_data)
        print('3----->Write OK')
    except IOError:
        print('write error')

def process_equipment_sensors(spark, input_data, output_data):
    '''
    Write parquet files
    on the gold path

    Parameters:
    input_data (str): Path to input data
    output_data (str): Path to output data
    '''

    try:
        df_data = spark.read.format('csv').\
        option('sep', ';').option('header', True).load(input_data)
        print('1->Read {} - OK'.format(input_data))
    except IOError:
        print('read error')

    df_data = df_data.select(col('equipment_id').cast('integer'),
                             col('sensor_id').cast('integer'))

    print('2--->Format and clean data {} - OK'.format(input_data))

    try:
        df_data.write.format('parquet').mode('overwrite').save(output_data)
        print('3----->Write OK')
    except IOError:
        print('write error')

def process_equipment(spark, input_data, output_data):
    '''
    Write parquet files
    on the gold path

    Parameters:
    input_data (str): Path to input data
    output_data (str): Path to output data
    '''
    try:
        df_data = spark.read.format('json').\
        option('multiLine', True).load(input_data)

        print('1->Read {} - OK'.format(input_data))
    except IOError:
        print('read error')

    print('2--->Format and clean data {} - OK'.format(input_data))

    try:
        df_data.write.format('parquet').mode('overwrite').save(output_data)
        print('3----->Write OK')
    except IOError:
        print('write error')

def main():
    '''
    Main Function
    '''
    spark = create_spark_session()

    process_equipment_failure_sensors(spark,
                                      'datalake/bronze/equipment_failure_sensors.log',
                                      'datalake/silver/equipment_failure_sensors')

    process_equipment(spark,
                      'datalake/bronze/equipment.json',
                      'datalake/silver/equipment/')

    process_equipment_sensors(spark,
                              'datalake/bronze/equipment_sensors.csv',
                              'datalake/silver/equipment_sensors/')

if __name__ == "__main__":
    main()
