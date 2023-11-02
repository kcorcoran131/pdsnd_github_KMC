import pandas as pd
import numpy as np
import time
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the file paths in DBFS format
file_path1 = 'dbfs:/user/hive/warehouse/wgubidev.db/kc_chicago'
file_path2 = 'dbfs:/user/hive/warehouse/wgubidev.db/kc_newyork'
file_path3 = 'dbfs:/user/hive/warehouse/wgubidev.db/kc_washington'

# Read the Delta tables into Spark DataFrames
spark_df1 = spark.read.format('delta').load(file_path1)
spark_df2 = spark.read.format('delta').load(file_path2)
spark_df3 = spark.read.format('delta').load(file_path3)

# Convert Spark DataFrames to Pandas DataFrames
pandas_df1 = spark_df1.toPandas()
pandas_df2 = spark_df2.toPandas()
pandas_df3 = spark_df3.toPandas()

# Work with the loaded Delta tables in the Pandas DataFrames (pandas_df1, pandas_df2, pandas_df3)

CITY_DATA = { 'chicago': 'pandas_df1','new york city': 'pandas_df2','washington': 'pandas_df3'}

def get_filters():
  """
  Asks user to specify a city, month, and day to analyze.

  Returns:
    (str) city - name of the city to analyze
    (str) month - name of the month to filter by, or "all" to apply no month filter
    (str) day - name of the day of week to filter by, or "all" to apply no day filter
  """
  print('Hello! Let\'s explore some US bikeshare data!')
  
  # get user input for city (chicago, new york city, washington).
  while True:
    cities= ['chicago','new york city','washington']
    city= input("\n Which city would you like to investigate? (Chicago, New York City, or Washington) \n").lower()
    if city in cities:
      break
    else:
      print("\n Please enter a valid city name, such as Chicago, New York City, or Washington")    

  # get user input for month (january, february, ... , june or none)
  while True:
    months= ['January','February','March','April','May','June','None']
    month = input("\n Okay! Which month would you like to investigate? (January, February, March, April, May, or June)? Type 'None' otherwise\n").title()
    if month in months:
      break
    else:
      print("\n Please enter a valid month.")    

  # get user input for day of week (monday, tuesday, ... sunday or none)
  while True:
    days= ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday','None']
    day = input("\n Sounds great! Which day of the week would you like to investigate? (Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday)? Type 'None' otherwise\n").title()         
    if day in days:
      break
    else:
      print("\n Please enter a valid day.")      
  print('-'*40)
  return city, month, day

def load_data(city, month, day):
  """
  Loads data for the specified city and filters by month and day if applicable.

  Args:
    (str) city - name of the city to analyze
    (str) month - name of the month to filter by, or "None" to apply no month filter
    (str) day - name of the day of week to filter by, or "None" to apply no day filter
  Returns:
    df - Pandas DataFrame containing city data filtered by month and day
  """

    
  # load data file into one dataframe
  df = pd.concat([pandas_df1, pandas_df2, pandas_df3])

  # convert the start time column to datetime format
  df['Start Time'] = pd.to_datetime(df['Start Time'])

  # extract month and day of week and create new columns
  df['month'] = df['Start Time'].dt.month
  df['day_of_week'] = df['Start Time'].dt.day


  # filter by month 
  if month != 'None':
    # use the index of the months list to get the corresponding int
    months = ['January', 'February', 'March', 'April', 'May', 'June']
    month = months.index(month)+1
    
    # filter by month to create the new dataframe
    df = df[df['month']==month] 

  # filter by day of week 
  if day != 'None':
    # filter by day of week to create the new dataframe
    df = df[df['day_of_week']==day]

  return df


def time_stats(df,month,day):
  """Displays statistics on the most frequent times of travel."""

  print('\nCalculating The Most Frequent Times of Travel...\n')
  start_time = time.time()

  # display the most common month
  if month =='None':
    common_month= df['month'].mode()[0]
    months= ['January','February','March','April','May','June']
    common_month= months[common_month-1]
    print("The most common month is",common_month)


  # display the most common day of week
  if day =='None':
    common_day= df['day_of_week'].mode()[0]
    print("The most common day of the week is", common_day)

  # display the most common start hour
  df['Start Hour'] = df['Start Time'].dt.hour
  common_hour = df['Start Hour'].mode()
  print("The most common starting hour is {}:00 hrs".format(common_hour))

  print("\nThis took %s seconds." % (time.time() - start_time))
  print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""
    print('\nCalculating the most common stations and trip...\n')
    start_time = time.time()

    print("Number of rows in DataFrame:", len(df))
    print("Columns in DataFrame:", df.columns)

    # display most commonly used start station
    common_start_station = df['Start Station'].mode()
    print("The most commonly used starting station is", common_start_station)

    # display most commonly used end station
    common_end_station = df['End Station'].mode()
    print("The most commonly used ending station is", common_end_station)

    # display most frequent combination of start station and end station trip
    df['combination'] = df['Start Station'] + " " + "to" + " " + df['End Station']
    common_combination = df['combination'].mode()
    print("The most frequent combination of starting and ending station is", common_combination)

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
  """Displays statistics on the total and average trip duration."""

  print('\nCalculating trip duration...\n')
  start_time = time.time()

  # Filter out rows with missing 'Trip Duration' values
  df = df.dropna(subset=['Trip Duration'])

  # display total travel time
  total_duration = df['Trip Duration'].sum()
  minute, second = divmod(total_duration, 60)
  hour, minute = divmod(minute, 60)
  print("The total trip duration was: {} hour(s) {} minute(s) {} second(s)".format(hour, minute, second))

  # display mean travel time
  if not df.empty:  # Check if there are rows left after filtering out NaN values
    average_duration = round(df['Trip Duration'].mean())
    m, sec = divmod(average_duration, 60)
    if m > 60:
      h, m = divmod(m, 60)
      print("The average trip duration is: {} hour(s) {} minute(s) {} second(s)".format(h, m, sec))
    else:
      print("The average trip duration is: {} minute(s) {} second(s)".format(m, sec))
  else: 
    print("No valid trip duration data available.")

  print("\nThis took %s seconds." % (time.time() - start_time))
  print('-'*40)


def user_stats(df, city):
    """Displays statistics on bikeshare users."""

    print('\nCalculating user statistics...\n')
    start_time = time.time()

    # Display counts of user types
    if 'User Type' in df:
        user_counts = df['User Type'].value_counts()
        print("The user types are:\n", user_counts)
    else:
        print("User Type data not available.")

    # Display counts of gender (not available for Washington)
    if city.title() == 'Chicago' or city.title() == 'New York City':
        if 'Gender' in df:
            gender_counts = df['Gender'].value_counts()
            print("\nThe counts of each gender are:\n", gender_counts)
        else:
            print("Gender data not available.")
    
    # Display earliest birth year
    if 'Birth Year' in df:
        earliest = int(df['Birth Year'].min())
        print("\nThe oldest user was born in the year", earliest)
    
    # Display most recent birth year
    if 'Birth Year' in df:
        most_recent = int(df['Birth Year'].max())
        print("The youngest user was born in the year", most_recent)
    
    # Display most common birth year
    if 'Birth Year' in df:
        common = int(df['Birth Year'].mode()[0])
        print("The largest number of users was born in the year", common)

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)

def display_data(df):

  while True:
    response=['yes','no']
    choice= input("Would you like to view 5 entries of raw data? Type 'yes' or 'no'\n").lower()
    if choice in response:
      if choice=='yes':
        start=0
        end=5
        data = df.iloc[start:end,:9]
        print(data)
      break     
    else:
      print("Please enter yes or no.")
  if  choice=='yes':       
    while True:
      choice_2= input("Would you like to view more trip data? Type 'yes' or 'no'\n").lower()
      if choice_2 in response:
        if choice_2=='yes':
          start+=5
          end+=5
          data = df.iloc[start:end,:9]
          print(data)
        else:    
          break  
      else:
        print("Please enter yes or no.")              


def main():
  while True:
    city, month, day = get_filters()
    df = load_data(city, month, day)
    time_stats(df,month,day)
    station_stats(df)
    trip_duration_stats(df)
    user_stats(df,city)
    display_data(df)

  
    restart = input('\nWould you like to restart? Enter yes or no.\n')
    if restart.lower() != 'yes':
        break

if __name__ == "__main__":
    main()

