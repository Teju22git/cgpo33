from pyspark.sql import *
#from pyspark import SparkContext
#sc = SparkContext("local", "First App")
from configparser import ConfigParser
from pyspark.sql.functions import *
def main():
   spark = SparkSession.builder. config("spark.jars","file:///C:/installers/Driver/postgresql-42.6.0.jar").appName("jdbc").master("local").getOrCreate()
   config = ConfigParser()
   config_path = "C:/Users/ytejeswa/PycharmProjects/pythonproject/File.properties"
   with open(config_path, "r") as config_file:
             content = config_file.read()
             config.read_string(content)

   properties = {

   "driver": config.get("database", "driver"),

   "user": config.get("database", "user"),

   "url": config.get("database", "url"),

   "password": config.get("database", "password")

   }
   #customer_table
   cust_path = 'C:/Users/ytejeswa/PycharmProjects/pythonproject/output_data/customers/'
   cust = spark.read.parquet(cust_path)
   cust.createOrReplaceTempView('Cust_temp')
   #cust.show()

   #item_table

   item_path = 'C:/Users/ytejeswa/PycharmProjects/pythonproject/output_data/items/'
   item = spark.read.parquet(item_path)
   item.createOrReplaceTempView('Item_temp')
   #item.show()

   #orders_table

   order_path = 'C:/Users/ytejeswa/PycharmProjects/pythonproject/output_data/orders/'
   order= spark.read.parquet(order_path)
   order.createOrReplaceTempView('Order_temp')
   #order.show()

   #order_details_table

   order_det_path = 'C:/Users/ytejeswa/PycharmProjects/pythonproject/output_data/order_details/'
   order_det = spark.read.parquet(order_det_path)
   order_det.createOrReplaceTempView('Order_det_temp')
   #order_det.show()

   #ship_to_table

   ship_to_path = 'C:/Users/ytejeswa/PycharmProjects/pythonproject/output_data/ship_to/'
   ship = spark.read.parquet(ship_to_path)
   ship.createOrReplaceTempView('Ship_temp')
   #ship.show()

   # saleperson_table

   salesperson_path = 'C:/Users/ytejeswa/PycharmProjects/pythonproject/output_data/salesperson/'
   sale = spark.read.parquet(salesperson_path)
   sale.createOrReplaceTempView('Sale_temp')
   #sale.show()

   # retriving data using spark.sql

   #spark.sql('select * from Cust_temp').show(5)
   #spark.sql('select * from Item_temp').show(5)
   #spark.sql('select * from Order_temp').show(5)
   #spark.sql('select * from Order_det_temp').show(5)
   #spark.sql('select * from Ship_temp').show(5)
   #spark.sql('select * from Sale_temp').show(5)

   return spark,properties


#----Monthly/Weekly Customer wise  orders

def one(sparks,property):
   first = '''select c.cust_id,c.cust_name, count(od.ORDER_ID)  as totalCount from Cust_temp c  
   left join Order_temp rd on (c.CUST_ID= rd.cust_id )
   left join Order_det_temp od on ( rd.ORDER_ID = od. ORDER_ID)
   left join Item_temp i on (od.ITEM_ID = i.ITEM_ID)
   group by c.cust_id,c.cust_name'''
   out1 = sparks.sql(first).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out1.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='first_table_outputs',mode = mode, properties=properties)
   out1.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/first_table_data')

   out1.show()

# ----Monthly/Weekly Customer wise sum of orders
def two(sparks,property):
   second = '''select cust_id , cust_name,sum( ITEM_QUANTITY*DETAIL_UNIT_PRICE) total_order from (
   select c.cust_id,c.cust_name, od.ITEM_ID, od.ITEM_QUANTITY, od.DETAIL_UNIT_PRICE  from Cust_temp c  
   left join Order_temp rd on (c.CUST_ID= rd.cust_id )
   left join Order_det_temp od on ( rd.ORDER_ID = od. ORDER_ID)
   left join Item_temp i on (od.ITEM_ID = i.ITEM_ID) ) p
   group by cust_id , cust_name'''
   out2 = sparks.sql(second).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out2.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='second_table',mode = mode, properties=properties)
   out2.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/second_table_data')

   out2.show()


# -- Item wise Total Order count
def three(sparks,property):
   third = '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(ITEM_QUANTITY) as total from Item_temp i
   left join Order_det_temp od on (od.ITEM_ID = i.ITEM_ID)
   group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by total desc'''
   out3 = sparks.sql(third).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out3.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='third_table',mode = mode, properties=properties)
   out3.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/third_table_data')


   out3.show()

  # -- Item name/category wise  Total Order count descending
def four(sparks,property):
    fourth = '''select i.CATEGORY , sum(od.ITEM_QUANTITY) as total from Item_temp i
    left join Order_det_temp od on (od.ITEM_ID = i.ITEM_ID)
    group by i.CATEGORY  order by total desc'''
    out4 = spark.sql(fourth).withColumn('Currentdate',current_date())
    mode = "overwrite"
    out4.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='fourth_table',mode = mode, properties=properties)
    out4.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/fourth_table_data')


    out4.show()

# -- Item wise total order amount in descending
def five(sparks,property):
   fifth = '''
   select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as total from Item_temp i
   left join Order_det_temp od on (od.ITEM_ID = i.ITEM_ID)
   group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by total desc'''
   out5 =sparks.sql(fifth).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out5.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='fifth_table',mode = mode, properties=properties)
   out5.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/fifth_table_data')


   out5.show()

# -- Item name/category wise  total order amount in descending
def six(sparks,property):
  sixth = '''select i.CATEGORY , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as summation from Item_temp i
   left join Order_det_temp od on (od.ITEM_ID = i.ITEM_ID)
    group by i.CATEGORY  order by summation desc '''
  out6 = sparks.sql(sixth).withColumn('Currentdate',current_date())
  mode = "overwrite"
  out6.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='sixth_table',mode = mode, properties=properties)
  out6.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/sixth_table_data')


  out6.show()

#--Salesman wise total order amt and incentive calculation for salesman(10%) for previous month

def seven(sparks,property):
   seventh = '''select s.SALESMAN_ID,  sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) tot_ord_amt,
   cast(sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) * 0.1 AS DECIMAL(10,2)) commission
   from Sale_temp s  
   left join Cust_temp c on (s.SALESMAN_ID = c.SALESMAN_ID)
   left join Order_temp rd on (c.CUST_ID= rd.cust_id )
   left join Order_det_temp od on ( rd.ORDER_ID = od.ORDER_ID)
   group by s.SALESMAN_ID '''
   out7 = sparks.sql(seventh).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out7.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='seventh_table',mode = mode, properties=properties)
   out7.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/seventh_table_data')

   out7.show()


#--Reports for Items not sold in previous month
def eight(sparks,property):
    eigth = '''select i.ITEM_ID,i.ITEM_DESCRIPTION from Item_temp i where
     i.ITEM_ID not in (select item_id from Order_det_temp od )'''
    out8 = sparks.sql(eigth).withColumn('Currentdate',current_date())
    mode = "overwrite"
    out8.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='eighth_table',mode = mode, properties=properties)
    out8.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/eigth_table_data')


    out8.show()


#--Report customer whose orders not shipped in previous month
def nine(sparks,property):
   ninth = '''select c.CUST_ID, c.cust_name from Cust_temp c where
    c.CUST_ID not in (select s.CUST_ID  from Ship_temp s)'''
   out9 = spark.sql(ninth).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out9.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='ninth_table',mode = mode, properties=properties)
   out9.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/ninth_table_data')

   out9.show()

#--All customer shipment whose address is not filled correctly(incorrect pincode) for previous month

def ten(sparks,property):
   tenth = '''select c.CUST_ID, c.cust_name from Cust_temp c
   join 
   Ship_temp s on (s.cust_id =c.cust_id)  where length(s.POSTAL_CODE) !=6 '''
   out10 = spark.sql(tenth).withColumn('Currentdate',current_date())
   mode = "overwrite"
   out10.write.jdbc(url = 'jdbc:postgresql://localhost:5432/Newdatabase' , table='tenth_table',mode = mode, properties=properties)
   out10.write.partitionBy('Currentdate').mode('overwrite').parquet('C:/Users/createtemp/tenth_table_data')


   out10.show()





if __name__ == '__main__':
    spark, properties = main()
    one(spark,properties)
    two(spark,properties)
    three(spark,properties)
    four(spark,properties)
    five(spark,properties)
    six(spark,properties)
    seven(spark,properties)
    eight(spark,properties)
    nine(spark,properties)
    ten(spark,properties)
