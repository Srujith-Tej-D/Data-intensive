from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date , year, avg , month

spark = SparkSession.builder \
    .appName("Import CSV from Hadoop") \
    .getOrCreate()

# Define the file path in Hadoop
file_path = "hdfs://hadoop-vm-1:54310/data-intesinve/airquality.csv"

# Read the CSV file into a PySpark DataFrame
air_df = spark.read.csv(file_path, header=True, inferSchema=True)

spark = SparkSession.builder \
    .appName("Import CSV from Hadoop") \
    .getOrCreate()

# Define the file path in Hadoop
file_path = "hdfs://hadoop-vm-1:54310/data-intesinve/waterquality.csv"

# Read the CSV file into a PySpark DataFrame
water_df = spark.read.csv(file_path, header=True, inferSchema=True)

# 1. Handling missing values
air_df = air_df.na.fill({"Geo Join ID": "unknown", "Geo Place Name": "unknown"})

# 2. Dropping the 'Message' column as it is completely null
air_df = air_df.drop("Message")

# 3. Converting 'Start_Date' to date format
air_df = air_df.withColumn("Start_Date", F.to_date("Start_Date", "MM/dd/yyyy"))

# 4. Ensuring 'Geo Join ID' is an integer (if necessary)
air_df = air_df.withColumn("Geo Join ID", air_df["Geo Join ID"].cast(IntegerType()))

# Example transformation: split into two columns if necessary
split_col = F.split(air_df['Time Period'], ' ')
air_df = air_df.withColumn('Period Descriptor', split_col.getItem(0))
air_df = air_df.withColumn('Period Year', split_col.getItem(1))

# Handle any specific data inconsistencies or further cleaning
# Example: standardize text fields
air_df = air_df.withColumn('Geo Place Name', F.initcap(F.col('Geo Place Name')))

# Optimizing DataFrame
# Example: repartition based on 'Geo Type Name' if it's a frequently filtered column
air_df = air_df.repartition("Geo Type Name")

air_df = air_df.withColumn('Period Year', F.split(air_df['Period Year'], '-')[0])

# Handling NULL values in 'Period Year' if needed
air_df = air_df.na.fill({'Period Year': 'Unknown'})

# Example of ensuring 'Period Year' only contains four-digit years
air_df = air_df.withColumn('Period Year', F.when(F.length('Period Year') == 4, air_df['Period Year']).otherwise('Unknown'))

# Refine the 'Period Year' cleanup
# This will extract year numbers from the 'Period Year' or fill with 'Unknown' if not found
air_df = air_df.withColumn('Period Year', F.regexp_extract('Period Year', r'(\d{4})', 1))

# Fill 'Unknown' for rows where 'Period Year' is empty after the extraction
air_df = air_df.withColumn('Period Year', F.when(F.col('Period Year') != '', F.col('Period Year')).otherwise('Unknown'))

# Optionally use the year from 'Start_Date' if 'Period Year' is 'Unknown'
air_df = air_df.withColumn('Period Year', F.when(F.col('Period Year') == 'Unknown', F.year('Start_Date')).otherwise(F.col('Period Year')))

#Map

# Ensure 'Start_Date' is of type Date if not already
air_df = air_df.withColumn("Start_Date", col("Start_Date").cast("date"))

# Extract year from 'Start_Date'
air_df = air_df.withColumn("Year", year("Start_Date"))

# Map phase: Prepare tuples (key-value pairs)
# Key: (Neighborhood, Year), Value: Data Value
mapped_data = air_df.select(col("Geo Place Name").alias("Neighborhood"),
                            col("Year"),
                            col("Data Value").alias("Value"))

# Reduce phase: Aggregate data by key (Neighborhood, Year), computing average Data Value
reduced_data = mapped_data.groupBy("Neighborhood", "Year").agg(avg("Value").alias("Average Data Value"))

# Sorting results for better visualization
final_result = reduced_data.orderBy("Neighborhood", "Year")


# Group by neighborhood and calculate average, ensuring it's a PySpark operation
neighborhood_averages = final_result.groupBy("Neighborhood").agg(avg("Average Data Value").alias("Overall Avg Data Value"))

# Get the top 5 neighborhoods based on average data value
top_neighborhoods = neighborhood_averages.orderBy(col("Overall Avg Data Value").desc()).limit(6)

# Fetch the top 5 neighborhoods as a list from PySpark DataFrame
top_neighborhoods_list = [row['Neighborhood'] for row in top_neighborhoods.collect()]

# Filter the data in 'final_result' for these top neighborhoods
top_neighborhoods_data = final_result.filter(final_result['Neighborhood'].isin(top_neighborhoods_list))


top_neighborhoods_data = final_result.filter((final_result['Neighborhood'].isin(top_neighborhoods_list)) & (final_result['Neighborhood'] != "Unknown"))
# Convert the filtered PySpark DataFrame to Pandas for visualization
top_neighborhoods_pd = top_neighborhoods_data.toPandas()

# Continue with Plotly visualization as previously described
import plotly.express as px

fig = px.line(top_neighborhoods_pd,
              x='Year',
              y='Average Data Value',
              color='Neighborhood',
              markers=True,
              title="Trends in Air Quality for Top 5 Neighborhoods Over Years")

fig.update_layout(xaxis_title="Year",
                  yaxis_title="Average Data Value (µg/m³)",
                  legend_title="Neighborhood")

#fig.show()

fig.write_image('Trends_in_Air_Quality_for_Top_5_Neighborhoods_Over_Years.png')


average_pollution = air_df.groupBy("Geo Place Name").agg(avg("Data Value").alias("Average Pollution"))

# Map each neighborhood to its average pollution value
mapped_data = average_pollution.rdd.map(lambda x: (x["Geo Place Name"], x["Average Pollution"]))


reduced_data = mapped_data.sortBy(lambda x: x[1], ascending=False)
# Assuming 'mapped_data' is your DataFrame from the previous map step, and already includes Neighborhood and Average Pollution columns
# Reduce step: sort by average pollution and take only the top 5


# Assuming 'mapped_data' is an RDD of tuples (Neighborhood, Average Pollution)
# Sort the RDD by the average pollution values in descending order and take the top 5
reduced_data = mapped_data.sortBy(lambda x: x[1], ascending=False).take(6)


import pandas as pd
import plotly.express as px

# Convert the list of tuples into a Pandas DataFrame
reduced_pandas_df = pd.DataFrame(reduced_data, columns=["Neighborhood", "Average Pollution"])

reduced_pandas_df = reduced_pandas_df[reduced_pandas_df['Neighborhood'].str.lower() != 'unknown']
fig = px.line(reduced_pandas_df,
              x='Neighborhood',  # Using Neighborhood as the x-axis
              y='Average Pollution',
              title='Average Pollution Levels by Top 5 Neighborhoods',
              labels={'Neighborhood': 'Neighborhood', 'Average Pollution': 'Average Pollution (µg/m³)'},
              markers=True)  # This adds markers to each data point

# Display the figure
#fig.show()
fig.write_image('Average_Pollution_Levels_by_Top_5_Neighborhoods.png')

from pyspark.sql.functions import col, when, regexp_replace, to_date, year, month

# Rename the column with a complex name for easier handling
water_df = water_df.withColumnRenamed("E.coli(Quanti-Tray) (MPN/100mL)", "Ecoli_MPN_per_100mL")

# Convert '<1' in Ecoli_MPN_per_100mL to '0' and remove non-numeric characters, then cast to integer
water_df = water_df.withColumn("Ecoli_Cast",
                               when(col("Ecoli_MPN_per_100mL") == "<1", 0)
                               .otherwise(regexp_replace(col("Ecoli_MPN_per_100mL"), "[^0-9]", ""))
                               .cast("integer"))

# Handle date format issues by converting string dates to actual date types and extracting year and month
water_df = water_df.withColumn("Formatted_Sample_Date", to_date(col("Sample Date"), "MM/dd/yyyy"))
water_df = water_df.withColumn("year", year("Formatted_Sample_Date"))
water_df = water_df.withColumn("month", month("Formatted_Sample_Date"))

# Drop rows with any missing values in critical columns and remove duplicates
water_df = water_df.na.drop(subset=["Ecoli_Cast", "year", "month", "Sample Site"])
water_df = water_df.dropDuplicates()

from pyspark.sql.functions import avg

# Example: Aggregate to calculate average Residual Free Chlorine by Sample Site and month
average_chlorine_df = water_df.groupBy("Sample Site", "year", "month").agg(
    avg("Residual Free Chlorine (mg/L)").alias("Average Chlorine")
)

# Show the results
#average_chlorine_df.show()

# Example: Handling non-numeric values in Turbidity similar to E.coli handling
water_df = water_df.withColumn("Turbidity_Clean",
                               when(col("Turbidity (NTU)") == "<0.10", 0)
                               .otherwise(regexp_replace(col("Turbidity (NTU)"), "[^0-9.]", ""))
                               .cast("float"))


#map

# Assuming the current year is 2023 and going back five years
filtered_df = water_df.filter((col("year") >= 2018) & (col("year") <= 2023))


def convert_value(value):
    if isinstance(value, str):
        try:
            # Attempt to convert string to float, removing any '<' characters indicating approximations
            return float(value.replace('<', '').strip())
        except ValueError:
            return 0.0
    elif isinstance(value, float):
        return value
    else:
        return 0.0

def map_function(row):
    site, year, month = row['Sample Site'], row['year'], row['month']
    turbidity = convert_value(row['Turbidity_Clean'])
    fluoride = convert_value(row['Fluoride (mg/L)'])
    return ((site, year, month), (turbidity, fluoride, 1))

mapped_data = filtered_df.rdd.map(map_function)

def reduce_function(a, b):
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2])

reduced_data = mapped_data.reduceByKey(reduce_function)

final_result = reduced_data.map(lambda x: (
    x[0],  # (Sample Site, Year, Month)
    x[1][0] / x[1][2] if x[1][2] != 0 else 0,  # Average Turbidity
    x[1][1] / x[1][2] if x[1][2] != 0 else 0   # Average Fluoride
)).collect()


# Create a DataFrame from the structured data
df_rs1 = pd.DataFrame({
    "Sample Site": [x[0][0] for x in final_result],
    "Year": [x[0][1] for x in final_result],
    "Month": [x[0][2] for x in final_result],
    "Average Turbidity": [x[1] for x in final_result],
    "Average Fluoride": [x[2] for x in final_result]
})

# Convert Year and Month into a single datetime column for better plotting
df_rs1['Date'] = pd.to_datetime(df_rs1[['Year', 'Month']].assign(DAY=1))

# Aggregate data by Sample Site and Year
df_site_year = df_rs1.groupby(['Sample Site', 'Year']).mean().reset_index()

# Create line plots for each water quality indicator (turbidity and fluoride) for each borough over the years
fig_turbidity = px.line(df_site_year, x='Year', y='Average Turbidity', color='Sample Site',
                        title='Trends in Turbidity Levels Across NYC Boroughs')
fig_fluoride = px.line(df_site_year, x='Year', y='Average Fluoride', color='Sample Site',
                        title='Trends in Fluoride Levels Across NYC Boroughs')

# Show the plots
#fig_turbidity.show()
fig_turbidity.write_image('Trends_in_Turbidity_Levels_Across_NYC_Boroughs.png')
#fig_fluoride.show()
fig_fluoride.write_image('Trends_in_Fluoride_Levels_Across_NYC_Boroughs.png')


# Convert DataFrame to RDD
rdd = water_df.rdd

# Define a function to check if a sample fails EPA standards
def sample_fail(sample):
    ecoli = sample["Ecoli_MPN_per_100mL"]
    coliform = sample["Coliform (Quanti-Tray) (MPN /100mL)"]
    return ecoli != "<1" or coliform != "<1"

# Map function to assign 1 if sample fails, else 0
failed_samples_rdd = rdd.map(lambda x: (1 if sample_fail(x) else 0, 1))

# Reduce function to sum up failed samples and total samples
failed_count, total_samples = failed_samples_rdd.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Calculate failure rate
failure_rate = (failed_count / total_samples) * 100

import plotly.graph_objs as go

# Calculate the percentage of failed samples
passed_count = total_samples - failed_count
failed_percentage = (failed_count / total_samples) * 100
passed_percentage = 100 - failed_percentage

# Define data
labels = ['Failed Samples', 'Passed Samples']
values = [failed_percentage, passed_percentage]

# Define colors
colors = ['#FF6347', '#7FFF00']

# Create a pie chart
fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4, marker=dict(colors=colors))])

# Update layout for better visualization
fig.update_layout(
    title='Samples Meeting EPA Standards',
    annotations=[dict(text=f'{failed_count}/{total_samples} Samples', showarrow=False)],
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)

# Show the plot
#fig.show()

fig.write_image('Samples_Meeting_EPA_Standards.png')



# Check column names in water DataFrame
water_df.printSchema()

# Check column names in air DataFrame
air_df.printSchema()

nyc_water_df = water_df
nyc_air_df = air_df


# Join the two DataFrames
joined_df = nyc_water_df.alias("water").join(nyc_air_df.alias("air"), col("water.year") == col("air.Year"))

# Perform further analysis
joined_df.select("water.Sample Site", "water.year", "air.Data Value", "water.Turbidity (NTU)") \
    .groupBy("water.year") \
    .agg({"air.Data Value": "mean", "water.Turbidity (NTU)": "mean"}) \
    .orderBy("water.year")

import plotly.graph_objs as go

# Data
years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"]
avg_turbidity = [0.8095, 0.6782, 0.7353, 0.7531, 0.6730, 0.7162, 0.6014, 0.7723]
avg_data_value = [21.1434, 18.1460, 20.3889, 15.5662, 24.7185, 14.4195, 14.7369, 14.7855]

# Create traces for Turbidity
trace_turbidity = go.Scatter(x=years, y=avg_turbidity, mode='lines+markers', name='Average Turbidity (NTU)', line=dict(color='blue'))

# Layout for Turbidity plot
layout_turbidity = go.Layout(title='Average Turbidity (NTU) over Years',
                              xaxis=dict(title='Year'),
                              yaxis=dict(title='Average Turbidity (NTU)'),
                              showlegend=True)

# Create plot for Turbidity
fig_turbidity = go.Figure(data=[trace_turbidity], layout=layout_turbidity)

# Create traces for Data Value
trace_data_value = go.Scatter(x=years, y=avg_data_value, mode='lines+markers', name='Average Air Data Value', line=dict(color='green'))

# Layout for Data Value plot
layout_data_value = go.Layout(title='Average Air Data Value over Years',
                              xaxis=dict(title='Year'),
                              yaxis=dict(title='Average Air Data Value'),
                              showlegend=True)

# Create plot for Data Value
fig_data_value = go.Figure(data=[trace_data_value], layout=layout_data_value)

# Show plots
#fig_turbidity.show()
fig_turbidity.write_image('Average_Turbidity_over_Years.png')
#fig_data_value.show()
fig_data_value.write_image('Average_Air_Data_Value_over_Years.png')
