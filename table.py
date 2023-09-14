from pyspark.sql import SparkSession



from configparser import ConfigParser

# printing helloo instead of spacesss
def main():


           #spark = SparkSession.builder.appName("jdbc").master("local").getOrCreate()
   spark = SparkSession.builder. config("spark.jars","file:///C:/installers/Driver/postgresql-42.6.0.jar").appName("jdbc").master("local").getOrCreate()


# Accessing the properties file which i have created under config folder i.e., Config.properties file

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



   table = ["customers","orders", "items", "salesperson", "order_details", "ship_to"]

   output_final = config.get("output","output_path")



   for i in table:
             data = spark.read.jdbc(url=properties["url"], table=i, properties=properties)
             data.show()
             data.write.parquet(output_final.format(str(i)))
             #data.write.parquet('C:/parquets/')
if __name__ == '__main__':

  main()
