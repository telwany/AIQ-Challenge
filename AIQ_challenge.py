
from google.colab import drive
import matplotlib.pyplot as plt
import pandas as pd
from ctypes import addressof
import requests
import time

#Change it if you will use any other data source
drive.mount('/content/drive', force_remount=True)

# Define the path to your CSV file in Google Drive
csv_file_path = "/content/drive/MyDrive/a/AIQ-Challenge/data/sales_data.csv"

# Load the CSV file into a DataFrame
sales_df = pd.read_csv(csv_file_path)

# Display the first few rows of the DataFrame
# wait 5 seconds
print("Sales Data :")
print("=====================================")
print(sales_df.head())
time.sleep(5)

# Check for missing values
missing_values = sales_df.isnull().sum()
print("Sales Data Missing Values")
print("=====================================")
print(missing_values)
time.sleep(5)

# Check data types
data_types = sales_df.dtypes
print("Sales Data Data types")
print("=====================================")
print(data_types)
time.sleep(5)

# Check for duplicates
duplicate_rows = sales_df[sales_df.duplicated()]
print("Duplicated Rows in Sales Data if Found")
print("=====================================")
print(duplicate_rows)

# Remove duplicates if exist
sales_df = sales_df.drop_duplicates()

# Summary statistics
print("Sales Data summary Stats")
print("=====================================")
summary_stats = sales_df.describe()
print(summary_stats)
time.sleep(5)

# Order ID : Histogram
plt.hist(sales_df['order_id'])
plt.xlabel('order id')
plt.ylabel('Frequency')
plt.title('Histogram of order id')
plt.show()
time.sleep(5)

# Customer ID : Histogram
plt.hist(sales_df['customer_id'])
plt.xlabel('customer id')
plt.ylabel('Frequency')
plt.title('Histogram of customer id')
plt.show()
time.sleep(5)

# Product ID : Histogram
plt.hist(sales_df['product_id'])
plt.xlabel('product id')
plt.ylabel('Frequency')
plt.title('Histogram of product id')
plt.show()
time.sleep(5)

# Quantity : Histogram
plt.hist(sales_df['quantity'])
plt.xlabel('quantity')
plt.ylabel('Frequency')
plt.title('Histogram of quantity')
plt.show()
time.sleep(5)

# Price : Histogram
plt.hist(sales_df['price'])
plt.xlabel('price')
plt.ylabel('Frequency')
plt.title('Histogram of price')
plt.show()
time.sleep(5)

# Checking date format
expected_format = '%Y-%m-%d'
# Try to convert the column to datetime with the expected format
try:
    sales_df['order_date'] = pd.to_datetime(sales_df['order_date'], format=expected_format)
    print("Date format is correct.")
except ValueError:
    print("Date format is incorrect.")

# Fetch user data change URL is needed
base_url = "https://jsonplaceholder.typicode.com"
users_endpoint = "/users"
url = f"{base_url}{users_endpoint}"
response = requests.get(url)
if response.status_code == 200:
    # Parse the JSON response
    users_data = response.json()
else:
    print(f"Failed to fetch user data. Status code: {response.status_code}")
    users_data = []  # Set an empty list in case of failure

users_df = pd.DataFrame(users_data)
print("Fetched Users Data")
print("=====================================")
print(users_df.head())
time.sleep(5)

# Create a new DataFrame with selected columns that will be used in analytics
curated_users_df = users_df[['id', 'name', 'username', 'email']]

# Apply the extract_lat_long function to the "address" column to be added to the final Data set
curated_users_df['latitude'] = users_df['address'].apply(lambda x: x.get('geo', {}).get('lat', None))
curated_users_df['longitude'] = users_df['address'].apply(lambda x: x.get('geo', {}).get('lng', None))

# Flatten User Data to be save in DB
flattened_users_df = users_df[['id', 'name', 'username', 'email']]
flattened_users_df['street'] = users_df['address'].apply(lambda x: x.get('street', {}))
flattened_users_df['suite'] = users_df['address'].apply(lambda x: x.get('suite', {}))
flattened_users_df['city'] = users_df['address'].apply(lambda x: x.get('city', {}))
flattened_users_df['zipcode'] = users_df['address'].apply(lambda x: x.get('zipcode', {}))
flattened_users_df['geo_lat'] = users_df['address'].apply(lambda x: x.get('geo', {}).get('lat', None))
flattened_users_df['geo_lng'] = users_df['address'].apply(lambda x: x.get('geo', {}).get('lng', None))
flattened_users_df['phone'] = users_df['phone']
flattened_users_df['website'] = users_df['website']
flattened_users_df['company_name'] = users_df['company'].apply(lambda x: x.get('name', {}))
flattened_users_df['company_catchphrase'] = users_df['company'].apply(lambda x: x.get('cathcphrase', {}))
flattened_users_df['company_bs'] = users_df['company'].apply(lambda x: x.get('bs', {}))
print("Users Data needed for Analytics")
print("===============================")
print(curated_users_df.head())
time.sleep(5)
print("Flattened Users Data to be saved in DB")
print("=====================================")
print(flattened_users_df.head())
time.sleep(5)

# Check for missing values
missing_values = curated_users_df.isnull().sum()
print("Users Missing Values :")
print("=====================================")
print(missing_values)
time.sleep(5)

# Check data types
data_types = curated_users_df.dtypes
print("Users Data Types :")
print("=====================================")
print(data_types)
time.sleep(5)


# Check for duplicates
duplicate_rows = curated_users_df[curated_users_df.duplicated()]
print("Users Duplicate records if found:")
print("=====================================")
print(duplicate_rows)
time.sleep(5)

# Remove duplicates
curated_users_df = curated_users_df.drop_duplicates()

# Summary statistics
summary_stats = curated_users_df.describe()
print("Users summary stats:")
print("=====================================")
print(summary_stats)
time.sleep(5)

# User ID : Histogram
plt.hist(curated_users_df['id'])
plt.xlabel('id')
plt.ylabel('Frequency')
plt.title('Histogram of user id')
plt.show()
time.sleep(5)

# latitude : Histogram
plt.hist(curated_users_df['latitude'])
plt.xlabel('latitude')
plt.ylabel('Frequency')
plt.title('Histogram of latitude')
plt.show()
time.sleep(5)

# longitude : Histogram
plt.hist(curated_users_df['longitude'])
plt.xlabel('longitude')
plt.ylabel('Frequency')
plt.title('Histogram of longitude')
plt.show()
time.sleep(5)

# Merge Sales Data with User Data
sales_users_df = pd.merge(sales_df, curated_users_df, left_on='customer_id', right_on='id', how='left')
print("Sales left outer Join Users Data:")
print("=====================================")
print(sales_users_df.head())
time.sleep(5)

# Check for missing values
missing_values = sales_users_df.isnull().sum()
print("Sales left outer Join Users Data missing data if found:")
print("=====================================")
print(missing_values)
time.sleep(5)

# Define your OpenWeatherMap API key
api_key = '83ac0cba25afe9820ee263f50de30f4e'
# Define a function to fetch weather data for a given latitude and longitude and temperature will be returned in celcuis degree
# add try and except to sleep in case the API hits exceeded the 1 minute limit
def get_weather_data(latitude, longitude,weather_id):
    base_url = 'https://api.openweathermap.org/data/2.5/weather?'
    params ='lat='+latitude+'&lon='+longitude+'&units=metric'+'&appid='+api_key
    retries = 0
    while retries <= 1000 :
      try :
        response = requests.get(base_url+params)
        if response.status_code == 200:
          weather_data = response.json()
          temperature = weather_data.get('main', {}).get('temp', None)
          weather_conditions = weather_data.get('weather', [])[0].get('description', None)
          return temperature, weather_conditions,weather_id,weather_id,weather_data.get('coord', {}),weather_data.get('weather', [])[0],weather_data.get('base', {}),weather_data.get('main', {}),weather_data.get('visibility', {}),weather_data.get('wind', {}),weather_data.get('clouds', {}),weather_data.get('dt', {}),weather_data.get('sys', {}),weather_data.get('timezone', {}),weather_data.get('id', {}),weather_data.get('name', {}),weather_data.get('cod', {})
        else:
          return None, None,None,None
      except Exception as e:
        print("sleep for 60 secs ")
        time.sleep(60)
        retries = retries + 1
    return None, None,None,None

weather_df=sales_users_df[['order_id']]
weather_df.rename(columns={'order_id': 'id'})
# Apply the get_weather_data function to your DataFrame to fetch weather information
sales_users_df['temperature'], sales_users_df['weather_conditions'],sales_df['weather_id'],weather_df['id'],weather_df['coord'],weather_df['weather'],weather_df['base'],weather_df['main'],weather_df['visibility'],weather_df['wind'],weather_df['clouds'],weather_df['dt'],weather_df['sys'],weather_df['timezone'],weather_df['id'],weather_df['name'],weather_df['cod'] = zip(*sales_users_df.apply(lambda row: get_weather_data(row['latitude'], row['longitude'],row.name), axis=1))

# Display the updated DataFrame with weather information
print("Sales left outer Join Users Data left outer join weather:")
print("=====================================")
print(sales_users_df.head())
time.sleep(5)
print("Sales Data to be saved in DB :")
print("=====================================")
print(sales_df.head())
time.sleep(5)
print("Weather Data before Flattening :")
print("=====================================")
print(weather_df.head())
time.sleep(5)

# Flatten weather Data to be saved in DB
flattened_weather_df = weather_df[['id']]
flattened_weather_df['lat'] = weather_df['coord'].apply(lambda x: x.get('lat', {}))
flattened_weather_df['lon'] = weather_df['coord'].apply(lambda x: x.get('lon', {}))
flattened_weather_df['description'] = weather_df['weather'].apply(lambda x: x.get('description', {}))
flattened_weather_df['weather_id'] = weather_df['weather'].apply(lambda x: x.get('id', {}))
flattened_weather_df['weather_main'] = weather_df['weather'].apply(lambda x: x.get('main', {}))
flattened_weather_df['weather_icon'] = weather_df['weather'].apply(lambda x: x.get('icon', {}))
flattened_weather_df['base'] = weather_df['base']
flattened_weather_df['temp'] = weather_df['main'].apply(lambda x: x.get('temp', {}))
flattened_weather_df['feels_like'] = weather_df['main'].apply(lambda x: x.get('feels_like', {}))
flattened_weather_df['temp_min'] = weather_df['main'].apply(lambda x: x.get('temp_min', {}))
flattened_weather_df['temp_max'] = weather_df['main'].apply(lambda x: x.get('temp_max', {}))
flattened_weather_df['pressure'] = weather_df['main'].apply(lambda x: x.get('pressure', {}))
flattened_weather_df['humidity'] = weather_df['main'].apply(lambda x: x.get('humidity', {}))
flattened_weather_df['sea_level'] = weather_df['main'].apply(lambda x: x.get('sea_level', {}))
flattened_weather_df['grnd_level'] = weather_df['main'].apply(lambda x: x.get('grnd_level', {}))
flattened_weather_df['visibility'] = weather_df['visibility']
flattened_weather_df['speed'] = weather_df['wind'].apply(lambda x: x.get('speed', {}))
flattened_weather_df['deg'] = weather_df['wind'].apply(lambda x: x.get('deg', {}))
flattened_weather_df['gust'] = weather_df['wind'].apply(lambda x: x.get('gust', {}))
flattened_weather_df['clouds'] = weather_df['clouds'].apply(lambda x: x.get('all', {}))
flattened_weather_df['dt'] = weather_df['dt']
flattened_weather_df['sunrise'] = weather_df['sys'].apply(lambda x: x.get('sunrise', {}))
flattened_weather_df['sunset'] = weather_df['sys'].apply(lambda x: x.get('sunset', {}))
flattened_weather_df['timezone'] = weather_df['timezone']
flattened_weather_df['city_id'] = weather_df['id']
flattened_weather_df['city_name'] = weather_df['name']
flattened_weather_df['cod'] = weather_df['cod']
print("Flattened Weather Data :")
print("=====================================")
print(flattened_weather_df.head())
time.sleep(5)

# Check for missing values in analytical Data set
missing_values = sales_users_df.isnull().sum()
print("Analytical Data set check for missing values :")
print("=====================================")
print(missing_values)
time.sleep(5)

# Calculate total sales amount per customer
total_sales_per_customer = sales_users_df.groupby('customer_id')['price'].sum()
print("Calculate total sales amount per customer :")
print("=====================================")
print(total_sales_per_customer)
time.sleep(5)

# total sales per customer : Graph
plt.figure(figsize=(10, 6))
total_sales_per_customer.plot(kind='bar', rot=0)
plt.xlabel('Customer ID')
plt.ylabel('Total Sales Amount')
plt.title('Total Sales Amount Per Customer')
plt.show()
time.sleep(5)

# average order quantity per product
average_order_quantity_per_product = sales_users_df.groupby('product_id')['quantity'].mean()
print("average order quantity per product :")
print("=====================================")
print(average_order_quantity_per_product)
time.sleep(5)

# average order quantity per product : Graph
plt.figure(figsize=(10, 6))
average_order_quantity_per_product.plot(kind='bar', rot=0)
plt.xlabel('Product ID')
plt.ylabel('Average Order Quantity')
plt.title('Average Order Quantity Per Product')
plt.show()
time.sleep(5)

#Top-Selling Products by Quantity
top_selling_products = sales_users_df.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
print("Top-Selling Products by Quantity :")
print("=====================================")
print(top_selling_products)
time.sleep(5)

#Top-Selling Products Quantity : Graph top 10 selling
plt.figure(figsize=(10, 6))
top_selling_products.head(10).plot(kind='bar', rot=0)
plt.xlabel('Product ID')
plt.ylabel('Total Sales Quantity')
plt.title('Top-Selling Products (Descending Order)')
plt.show()
time.sleep(5)

#Top-Selling Products by Price
top_selling_products = sales_users_df.groupby('product_id')['price'].sum().sort_values(ascending=False)
print("Top-Selling Products by Price :")
print("=====================================")
print(top_selling_products)
time.sleep(5)

#Top-Selling Products Price : Graph top 10 selling
plt.figure(figsize=(10, 6))
top_selling_products.head(10).plot(kind='bar', rot=0)
plt.xlabel('Product ID')
plt.ylabel('Total Sales Price')
plt.title('Top-Selling Products (Descending Order)')
plt.show()
time.sleep(5)

#Top-Selling Customer by Price
top_selling_customer = sales_users_df.groupby('customer_id')['price'].sum().sort_values(ascending=False)
print("Top-Selling Customer by Price :")
print("=====================================")
print(top_selling_customer)
time.sleep(5)

#Top-Selling customer Price : Graph top 10 selling
plt.figure(figsize=(10, 6))
top_selling_customer.head(10).plot(kind='bar', rot=0)
plt.xlabel('Product ID')
plt.ylabel('Total Sales Price')
plt.title('Top-Selling customer (Descending Order)')
plt.show()
time.sleep(5)

#Top-Selling Customer by Quantity
top_selling_customer = sales_users_df.groupby('customer_id')['quantity'].sum().sort_values(ascending=False)
print("Top-Selling Customer by Quantity :")
print("=====================================")
print(top_selling_customer)
time.sleep(5)

#Top-Selling Customer Quantity : Graph top 10 selling
plt.figure(figsize=(10, 6))
top_selling_customer.head(10).plot(kind='bar', rot=0)
plt.xlabel('Customer ID')
plt.ylabel('Total Sales Quantity')
plt.title('Top-Selling Customers (Descending Order)')
plt.show()
time.sleep(5)

# Monthly Sales Trends Price
sales_users_df['order_date'] = pd.to_datetime(sales_users_df['order_date'])
monthly_sales = sales_users_df.groupby(sales_users_df['order_date'].dt.to_period('M'))['price'].sum()
# Monthly Sales Trends Price : Graph
plt.figure(figsize=(12, 6))
monthly_sales.plot(kind='line', marker='o')
plt.xlabel('Month')
plt.ylabel('Total Sales Amount')
plt.title('Monthly Sales Trends')
plt.xticks(rotation=45)
plt.grid()
plt.show()
time.sleep(5)


# Monthly Sales Trends Quantity
sales_users_df['order_date'] = pd.to_datetime(sales_users_df['order_date'])
monthly_sales = sales_users_df.groupby(sales_users_df['order_date'].dt.to_period('M'))['quantity'].sum()
# Monthly Sales Trends Quantity : Graph
plt.figure(figsize=(12, 6))
monthly_sales.plot(kind='line', marker='o')
plt.xlabel('Month')
plt.ylabel('Total Sales Quantity')
plt.title('Monthly Sales Trends')
plt.xticks(rotation=45)
plt.grid()
plt.show()
time.sleep(5)

# Monthly Sales per weathe
avg_sales_per_weather_condition = sales_users_df.groupby('weather_conditions')['price'].mean()
# Monthly Sales Trends Price
plt.figure(figsize=(10, 6))
avg_sales_per_weather_condition.plot(kind='bar', rot=45)
plt.xlabel('Weather Conditions')
plt.ylabel('Average Sales Amount')
plt.title('Average Sales Amount Per Weather Condition')
plt.show()
time.sleep(5)

# Monthly Sales Trends quantity
avg_sales_per_weather_condition = sales_users_df.groupby('weather_conditions')['quantity'].mean()
# Monthly Sales Trends quantity
plt.figure(figsize=(10, 6))
avg_sales_per_weather_condition.plot(kind='bar', rot=45)
plt.xlabel('Weather Conditions')
plt.ylabel('Average Sales Amount')
plt.title('Average Sales Amount Per Weather Condition')
plt.show()
time.sleep(5)


# Total Revenue Per Product by product ID
sales_users_df['total_revenue'] = sales_users_df['quantity'] * sales_users_df['price']
total_revenue_per_product = sales_users_df.groupby('product_id')['total_revenue'].sum()
print("Total Revenue Per Product by product ID :")
print("=====================================")
print (total_revenue_per_product)
time.sleep(5)
# Total Revenue Per Product : Graph
plt.figure(figsize=(10, 6))
total_revenue_per_product.plot(kind='bar', rot=0)
plt.xlabel('Product ID')
plt.ylabel('Total Revenue')
plt.title('Total Revenue Per Product')
plt.show()
time.sleep(5)

#Sales by Day of the Week : Graph
sales_users_df['day_of_week'] = sales_users_df['order_date'].dt.day_name()
sales_by_day_of_week = sales_users_df.groupby('day_of_week')['price'].sum()
plt.figure(figsize=(10, 6))
sales_by_day_of_week.plot(kind='bar', rot=0)
plt.xlabel('Day of the Week')
plt.ylabel('Total Sales Amount')
plt.title('Sales by Day of the Week')
plt.show()     
time.sleep(5)

# Segment customers based on their order frequency
def customer_segmentation(row):
    if row['order_frequency'] <= pd.Timedelta(days=30):
        return 'New Customer'
    elif pd.Timedelta(days=30) < row['order_frequency'] <= pd.Timedelta(days=90):
        return 'Returning Customer'
    else:
        return 'Loyal Customer'
sales_users_df.sort_values(by=['customer_id', 'order_date'], inplace=True)
sales_users_df['order_frequency'] = sales_users_df.groupby('customer_id')['order_date'].diff()
sales_users_df['customer_segment'] = sales_users_df.apply(customer_segmentation, axis=1)
customer_segment_counts = sales_users_df['customer_segment'].value_counts()

plt.figure(figsize=(8, 8))
plt.pie(customer_segment_counts, labels=customer_segment_counts.index, autopct='%1.1f%%', startangle=90)
plt.title('Customer Segmentation')
plt.show()
time.sleep(5)
