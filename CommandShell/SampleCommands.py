# load the csv into a dataframe

df = spark.read.format("csv").option("header","true").load('Data/ProductPriceList.csv')

# show the data

df.show()

# show rows where SalePrice is greater that 100

df.where("SalePrice > 100").select("Title").show()

# Include title and saleprice

df.where("SalePrice > 100").select("Title", "SalePrice").show()