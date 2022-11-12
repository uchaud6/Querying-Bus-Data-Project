# Umar Chaudhry
# uchaud6@uic.edu


import sqlite3 as sql
import pandas as pd
import matplotlib.pyplot as plt


def convert():
    "Replicate the data from 'bus_data.csv' to 'bus_data.db'"
    # 'data' is a pandas dataframe containing 'bus_data.csv' data
    data = pd.read_csv("bus_data.csv")
    # 'connection' is a sql object created from the method 'sql.connect()' and this
    # method will create the database file given if the database doesnt exist
    # in this case the database file is being created
    # 'cursor' is another sql object created from the method of the variable
    # 'connection' and this will allow for us to query the database using this cursor
    # object
    connection = sql.connect("bus_data.db")
    cursor = connection.cursor()

    # we want to replicate the csv data so we will create a new table, buses, that
    # will contain the same columns as the headers for 'bus_data.csv'
    cursor.execute(
        """CREATE TABLE buses (route TEXT, date TEXT, daytype TEXT, rides INTEGER)""")
    # by iterating over the range of the length of 'data' we can iterate over 'data'
    # by index so 'idx' stands for index
    for idx in range(len(data)):
        # at each index of 'data' we're retrieving one row of data that corresponds
        # to the values under each header

        # we can get the route value for the row of data by this two index process
        # of using the header and the index then saving it to 'route'
        # this process is the same for the other data values that are assigned to
        # 'date', 'daytype', and 'rides'

        # note we must do some extra formating for 'route','date', and 'daytype' because
        # these variables contain strings and TEXT data values need to be formatted a
        # special way when inserted into a database file but 'rides' is an INTEGER
        # value so no need to do that for 'rides'
        route = data["route"][idx]
        route = "'" + route + "'"
        date = data["date"][idx]
        date = "'" + date + "'"
        daytype = data["daytype"][idx]
        daytype = "'" + daytype + "'"
        rides = data["rides"][idx]

        # we can insert this row of data into the database with this command below
        cursor.execute("INSERT INTO buses VALUES ({}, {}, {}, {})".format(
            route, date, daytype, rides))

    # the for loop has been completed so all the data has been replicated
    # so we should commit the progress made and close the database file with
    # these two methods
    connection.commit()
    connection.close()

# Question: How much memory did the csv file take up? How much memory does the
# new database file take?

# 'bus_data.csv' takes 19,854 KB while 'bus_data.db' takes 24,664 KB


def route_data(route):
    """Given a bus route, print the average 
    daily number of riders for that route and percentage of days 
    where that route is under heavy use """
    # 'connection' is a sql object created from the method 'sql.connect()' and this
    # method will allow us to connect to the database assuming it exists in this case
    # 'cursor' is another sql object created from the method of the variable
    # 'connection' and this will allow for us to query the database using this cursor
    # object

    # we need to do some string formatting to 'route' as route is a TEXT datatype in
    # the database and there are routes containing letters with numbers
    route = "'" + str(route) + "'"
    connection = sql.connect("bus_data.db")
    cursor = connection.cursor()
    # Using 'AVG()' we can take the average rides of the buses with the selected
    # route using this command below
    cursor.execute(
        "SELECT AVG(rides) FROM buses WHERE route = {}".format(route))
    # we can then retrieve the average by using the '.fetchall()' method
    # and then doing the proper indexing of the return value of this method
    # and then finally it makes sense to round this value I believe because
    # a number of rides makes sense to be a whole number and this average value
    # is assigned to 'average_rides'
    average_rides = round(cursor.fetchall()[0][0])
    # we can count the total number of days that the selected route is used
    # using 'COUNT()'
    cursor.execute(
        "SELECT COUNT(rides) FROM buses WHERE route = {}".format(route))
    # we retrieve this value of the total number of days that the route is used
    # by calling '.fetchall()' and then indexing and we save this total to 'days_used'
    days_used = cursor.fetchall()[0][0]
    # we can count the number of days where the selected route is used and under
    # heavy use by using 'COUNT()' on the days where the number of rides in the day
    # is greater than or equal to 1200 with the command below
    cursor.execute(
        "SELECT COUNT(rides) FROM buses WHERE route = {} AND rides >= 1200".format(route))
    # we retrieve this value of number of days under heavy use
    # by calling '.fetchall()' and then indexing and we save this total to 'heavy_uses'
    heavy_uses = cursor.fetchall()[0][0]
    # we can calculate the percentage of days where that route is under
    # heavy use by dividing the number of days the route is under heavy use
    # by the total number of days the route is used and then multiplying that
    # quotient by 100 and then if we round the percentage to 2 decimals it looks nice
    # to look at and we do this by the associated variables and assign the final computed
    # value to 'heavy_usge_percent'
    heavy_usge_percent = round((heavy_uses / days_used) * 100, 2)
    # we have retrieved all the desired data so we can close the connection to the database
    connection.close()
    # print results
    print("The Average Number of Daily Riders for route {} is {}".format(
        route, average_rides))
    print("The Heavy Use Percentage for route {} is {}".format(
        route, heavy_usge_percent))


def yr_sum(*args):
    """Given some number of years, calculate the sum total of all rides for the
    specified years"""
    # 'connection' is a sql object created from the method 'sql.connect()' and this
    # method will allow us to connect to the database assuming it exists in this case
    # 'cursor' is another sql object created from the method of the variable
    # 'connection' and this will allow for us to query the database using this cursor
    # object
    conn = sql.connect("bus_data.db")
    cursor = conn.cursor()
    # 'total' is initalized to 0 as it will be adding on yearly totals of bus riders
    # to itself in the upcoming for loop
    total = 0
    # note that 'args' is a list of given years, so 'year' will be one of the given
    # years by iterating over 'args'
    for year in args:
        # we can compute the total number of rides in a year by selecting the
        # entries where the year is equal to 'year' by using 'LIKE' and then
        # we can use 'SUM()' to compute the total number of rides in all those
        # yearly entries
        cursor.execute(
            "SELECT SUM(rides) FROM buses WHERE date LIKE '%{}'".format(year))
        # we retrieve that total number of rides in the given year through
        # '.fetchall()' and indexing and then we add on that value to 'total'
        # as we are calculating the sum of all the rides in the given years
        total += cursor.fetchall()[0][0]

    # 'years_str' is a string that will contain all the years given
    # so we can mention which years we've computed the sum for the total rides
    # in our printed results
    years_str = ""
    # 'year' is used the same way as before
    for year in args:
        # these values in 'args' are integers so we must convert them into strings
        # for string concatenation and then add appropriate spacing between characters
        years_str += str(year) + " "

    # print the results
    print("The sum total of riders in the years, {},is {}".format(years_str, total))


def my_func():
    "Display Graphs for Maximum and Average Bus Riders Per Year"

    # 'connection' is a sql object created from the method 'sql.connect()' and this
    # method will allow us to connect to the database assuming it exists in this case
    # 'cursor' is another sql object created from the method of the variable
    # 'connection' and this will allow for us to query the database using this cursor
    # object
    conn = sql.connect("bus_data.db")
    cursor = conn.cursor()
    # years of data range from 2001 to 2022 so we can create a list that contains
    # a range of these years using range() and save this to 'years'
    years = list(range(2001, 2023))
    # we can iterate over 'years' so every iteration 'year' will be a year that
    # is included in the database
    for year in years:
        # issue a command to select the maximum number of rides for each year
        # using 'LIKE' and 'MAX()' in the command below
        cursor.execute(
            """SELECT MAX(rides) FROM buses WHERE date LIKE '%{}'""".format(year))
        # we retrieve this value of the maximum number of rides for one of the
        # years in the database by '.fetchall()' and then indexing and store this value
        # to 'rides_max'
        rides_max = cursor.fetchall()[0][0]
        # plot 'rides_max' and 'year' to create a plot of years and their associated
        # maximum number of rides
        plt.plot(year, rides_max, "ro")
    # add corresponding titles and labels to the plot and show the plot
    plt.title("Maximum Recorded Bus Riders Per Year")
    plt.xlabel("Years")
    plt.ylabel("Riders")
    plt.show()
    # 'year' is used in this for loop as before
    for year in years:
        # compute the average number of rides for each year in the database
        # by using 'LIKE' and 'AVG()' in the command below
        cursor.execute(
            """SELECT AVG(rides) FROM buses WHERE date LIKE '%{}'""".format(year))
        # retrieve this average number of rides in a year by using '.fetchall()'
        # and indexing and store this value to 'rides_avg'
        rides_avg = cursor.fetchall()[0][0]
        # plot 'rides_avg' and 'year' to create a plot of years and their associated
        # average number of rides
        plt.plot(year, rides_avg, "ro")
    # add corresponding titles and labels to the plot and show the plot
    plt.title("Average Recorded Bus Riders Per Year")
    plt.xlabel("Years")
    plt.ylabel("Riders")
    plt.show()
# Results:

# I wanted to see how these two topics of data, average number of riders per year
# and maximum number of riders per year, would fluctuate over the years and be impacted
# in the Corona virus pandemic time period. When it came to the average number of riders per
# year, I noticed how the average fluctuated around the range of 6000 to 7000 riders
# from 2001 to 2015 and then it dropped lower than 6000 going past 2016 and then there was
# a huge drop down to under 3000 riders in the year of 2020 and 2021. A slight increase
# going past 3000 but much less than 4000 riders in 2022 but its important to note the dataset
# does not give data past July, 2022 so theres missing data for 2022 and the average bus riders
# per year is looking to make a rise. The steep drop in average number of riders in 2020 shows
# that the corona virus skewed the average number of riders per year. When it came to
# the maximum number of riders per year, it
# was very interesting to see how large the maximum could get in comparison with the average
# number of riders, for example, in the years from 2000 to 2005, the maximum number of
# riders was over 40,000, whereas the average number of riders for 2001 was just above 7000.
# However, looking past 2005, a decreasing trend started where
# one can notice the maximums getting smaller and smaller until 2020 where the maximum
# was 25,000 riders and then the steepest drop occured for 2021 where the maximum
# number of riders was less than 15,000 and a similar low maximum is shared for 2022. It's
# clear that the maximum number of riders was skewed by the Corona virus pandemic with the
# maximum drop in 2021 and 2022 but there seems to be another contributor or reason for the
# years prior where a gradual decreasing maximum trend started.


def update():
    """Decrease the value of rides in all the entries in the database with the daytype
    value of A by 10%"""

    # Since we are updating the database, it's best practice to create a backup database
    # to contain the data before we do any updates
    # 'orgl_conn' and 'orgl_cursor' are the original connection and cursor objects
    # for 'bus_data.db' while 'backup_conn' and 'backup_cursor' are the connection
    # and cursors objects used to create a backup database file
    orgl_conn = sql.connect("bus_data.db")
    orgl_cursor = orgl_conn.cursor()
    backup_conn = sql.connect("bus_data_backup.db")
    backup_cursor = backup_conn.cursor()
    # issue a command to create a table in 'bus_data_backup.db' that replicates
    # the table in 'bus_data.db'
    backup_cursor.execute(
        """CREATE TABLE buses (route TEXT, date TEXT, daytype TEXT, rides INTEGER)""")
    # issue a command to select all the data from 'bus_data.db'
    orgl_cursor.execute("SELECT * FROM buses")
    # store all the data from  'bus_data.db' to 'data'
    data = orgl_cursor.fetchall()
    # using the execution command '.executemany()' along with '?' placeholders
    # we can input all the entries in 'data' to the backup database file
    backup_cursor.executemany("""INSERT INTO
                              buses VALUES (?,?,?,?)""", data)
    # we have successfully created a backup database so we can commit and close
    # the file
    backup_conn.commit()
    backup_conn.close()
    # issue a command to update the entries in the database with a daytype of A
    # that will decrease the rides by 10% rounded down and use CAST(.... as int) to
    # floor down the value of (rides - rides*0.1) to an integer this is the same
    # process as when one does int(some float) to get a floored integer ex. int(3.9) = 3
    orgl_cursor.execute(
        "UPDATE buses SET rides = CAST(rides - rides*0.1 as int) where daytype = 'A'")
    # we have updated the file so we can commit and close
    orgl_conn.commit()
    orgl_conn.close()
