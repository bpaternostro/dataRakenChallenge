import findspark
import pyspark
import sys
import logging

from pyspark.sql import Row,SQLContext,SparkSession

# this is to run a local instance of the cluster with a single core
# spark = pyspark.SparkContext('local[*]')
# sqlContext = SQLContext(spark)

# Select an APP 'Challenge'
spark = SparkSession.builder.appName('challenge').getOrCreate()
sparkContext=spark.sparkContext

from core import utils as util_o

logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')
util=util_o.Utils(logging)

# This is to find a local instance of the cluster to make tests. Should it be in another way working in a productive environment (remote)
#findspark.init("C:\Program Files\spark-3.1.1-bin-hadoop2.7")

# Execute process 01
util.execute_algoritmo01()

# Execute 05_a
util.execute_algoritmo_05_a()

# Creates a RDD with tabla tblPrmKey
logging.info("Creating RDD with table tblPrmKey")
# creates a spark data frame from pandas and then create the rdd
rdd_05_b=util.get_rdd(sp=spark,table_name="tblPrmKey",from="Pandas")

# Execute 05_D subPrmErnDates
util.execute_algoritmo_05_c()

# Execute 05_E subPrmErnPrem*/
util.execute_algoritmo_05_e()

# Execute 05_F
util.execute_algoritmo_05_f_0()

# Creates a RDD with tabla PrmErnDateRanges
logging.info("Creating RDD with table PrmErnDateRanges")
rdd_05_f=util.get_rdd(sp=spark,table_name="PrmErnDateRanges",from="Pandas")

# Creates a RDD with tabla tblPrmErn3
logging.info("Creating RDD with table tblPrmErn3")
rdd_05_f_1=util.get_rdd(sp=spark,table_name="tblPrmErn3",from="Pandas")

# Execute 05_H_I Se devenga
util.execute_algoritmo_05_h_i()

# Execute 05_J crea la tabla tblPrmErn
util.execute_algoritmo_05_j()

# Execute 06 modPrmSum
util.execute_algoritmo_06_a()

# Execute ALGORITMO - 06_B
util.execute_algoritmo_06_b()

# Creates a RDD for final process
logging.info("Creating RDD with final tblPrmSum table")
rdd_tblPrmSum=util.get_rdd(sp=spark,table_name="tblPrmSum",from="Pandas")

#Converts to dataFrame
df=rdd_tblPrmSum.toDF()

# Execute query to get : 'para calcular la prima neta y cedida del mes a cerrar para cuadrar contra Finanzas'
final_df=df.select(util.get_query_final_process)

final_df.printSchema()

if util.get_status_process():
    logging.debug('Process was executed successfully')    
else:
    logging.error('Process was executed unsuccessfully')
    
