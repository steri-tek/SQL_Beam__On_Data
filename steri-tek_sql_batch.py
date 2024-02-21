#!/usr/bin/env python3

import pymssql
import operator
from dataclasses import dataclass
import time
import sys

from datetime import datetime, timedelta
DEBUG = True
DEBUG_VERBOSE = False

events = []


@dataclass
class Mevex_SQL_Scraping:

    """"

        Class: Mevex_SQL_Scraping
        Author: James Kelly
        Date: 17-May-2023

        Description:

        This class uses the ...

    """""

    def __init__(self, database):

        self.database = database
        self.user = "sa"
        self.host = "10.2.10.200"
        self.password = "sql69jk"

        self.conn = ""
        self.cursor = ""

    def sql_connect(self):

        self.conn = pymssql.connect(self.host, self.user, self.password, self.database, tds_version="7.0")
        self.cursor = self.conn.cursor(as_dict=True)

    def sql_close(self):

        self.conn.close()

    def get_events_between_dates(self, start_dt, stop_dt, beam_number):

        select_string = 'select BEAM_ON, LOADING_BATCH, LOADING_BATCH_QUANTITY, LOADING_UBC_SPEED_1, ' \
                        'DateAndTime ' \
                        'FROM dbo.ProcessControllerTags Where DateAndTime between ' \
                        '\'' + start_dt + '\' AND ' \
                        '\'' + stop_dt + '\' order by DateAndTime ASC'

        if beam_number == 1:

            select_string = 'select BEAM_ON, DateAndTime ' \
                            'FROM dbo.Ebeam1Tags Where DateAndTime between ' \
                            '\'' + start_dt + '\' AND ' \
                                              '\'' + stop_dt + '\' order by DateAndTime ASC'

        if beam_number == 2:

            select_string = 'select BEAM_ON, DateAndTime ' \
                            'FROM dbo.Ebeam2Tags Where DateAndTime between ' \
                            '\'' + start_dt + '\' AND ' \
                                              '\'' + stop_dt + '\' order by DateAndTime ASC'

        self.cursor.execute(select_string)

        results = self.cursor.fetchall()

        return results

    def get_product_code(self, sbn):

        """
        SELECT ProductCode
        FROM[110056].[dbo].[Products]
        WHERE
        ProductID = (
            SELECT ProductID
            FROM[110056].[dbo].[SBNs_Products]
            WHERE SbnID = (
            SELECT TOP 1 SbnID
            FROM[110056].[dbo].[SBNs]
            WHERE SBN = 50379));
        """

        select_string = 'select ProductCode ' \
                        'FROM dbo.Products Where ProductID = ' \
                        '( select ProductID from dbo.SBNs_Products where SbnID = ' \
                        '( select top 1 SbnID from dbo.SBNs where SBN = ' + sbn + \
                        '))'

        self.cursor.execute(select_string)

        results = self.cursor.fetchall()

        for row in results:

            pc = row['ProductCode']

        return pc

    def get_batch_data(self, data_list):

        # Initialize cached data to first entry
        cached_batch = data_list[0]['LOADING_BATCH']

        for row in data_list:

            batch = row['LOADING_BATCH']
            batch_quantity = row['LOADING_BATCH_QUANTITY']
            date_and_time = row['DateAndTime']
            beam_on = row['BEAM_ON']
            ubc_speed = row['LOADING_UBC_SPEED_1']
            # product_code = row['LOADING_PRODUCT_CODE_1']

            # print(row)

            if batch is not None:

                batch = int(batch)
                batch_quantity = int(batch_quantity)

                if batch != cached_batch:

                    print("batch != cached_batch \n")

                    if batch != -1:
                        print("\nBatch Start @ : ", date_and_time, "   ", end="")
                        print("Batch = ", batch, end="")
                        print("  Batch Quantity = ", batch_quantity, end="")
                        batch_start_info = "batch " + str(batch) + " started. batch quantity: " + str(batch_quantity) + \
                                           " UBC Speed: " + str(round(ubc_speed, 2))
                        events.append({"timestamp": date_and_time, "event_text": batch_start_info, "batch": batch})

                    # If the following is True then the batch just finished
                    # as the batch just transitioned to -1
                    if batch == -1:
                        print("  Batch Finished @ ", date_and_time, "    ")
                        events.append({"timestamp": date_and_time, "event_text": "batch finished"})

                cached_batch = batch

    def get_beam_data(self, data_list):

        first_iteration = True

        # Initialize cached_beam_on to first entry prior to entering loop
        cached_beam_on = data_list[0]['BEAM_ON']
        print("Cached Beam On set to %s" % cached_beam_on)

        # Initialize total beam time prior to entering loop
        total_beam_time = timedelta(0, 0, 0000)

        # Beam On Check

        for row in data_list:

            beam_on = row['BEAM_ON']
            date_and_time = row['DateAndTime']

            year = date_and_time.year
            month = date_and_time.month
            day = date_and_time.day
            end_of_day = datetime(year, month, day, 23, 59, 59)

            # Throw away values in the database that are None
            if beam_on is not None:

                # Case to catch when beam is on when this routine starts - sets start time
                if beam_on and cached_beam_on and first_iteration:
                    start = datetime.fromisoformat(str(date_and_time))
                    first_iteration = False

                # If beam state changed
                if beam_on is not cached_beam_on:
                    # If beam_on then beam was previously off
                    if beam_on:

                        start = datetime.fromisoformat(str(date_and_time))
                        events.append({"timestamp": start, "event_text": "1"})

                    # else beam was previously on and has transitioned to off
                    else:

                        stop = datetime.fromisoformat(str(date_and_time))
                        beam_time = stop - start
                        total_beam_time = total_beam_time + beam_time
                        events.append({"timestamp": stop, "event_text": "0 %s" % total_beam_time})

                    # update cached value
                    cached_beam_on = beam_on

                # If Beam is on at 23:59:59 then tally the time since start
                if beam_on and date_and_time == end_of_day:

                    print("Beam ON at end of day")
                    stop = datetime.fromisoformat(str(date_and_time))
                    beam_time = stop - start
                    total_beam_time = total_beam_time + beam_time
                    events.append({"timestamp": stop, "event_text": "0 %s" % total_beam_time})


def main():

    """This is the main execution unit."""

    print("Timestamp, E-Beam 1 Beam Status, E-Beam 2 Beam Status")

    start_hour = 6
    start_minute = 0
    start_second = 0
    start_millisecond = 0

    stop_hour = 6
    stop_minute = 0
    stop_second = 0
    stop_millisecond = 0

    #starting_date = datetime.now() - timedelta(days=1, hours=0)
    starting_date = datetime(2024, 2, 14, 6, 00, 00, 000000)
    #print("Starting Date ", starting_date)

    target_date = starting_date

    trends_sql = Mevex_SQL_Scraping("OpcDataLogging")
    #data_sql = Mevex_SQL_Scraping("110056")

    trends_sql.sql_connect()
    #data_sql.sql_connect()


    i = 0

    while i < 1:

        #print("Target Date is ", target_date)

        # extract year, month, and day from target date

        start_year = target_date.year
        start_month = target_date.month
        start_day = target_date.day
        stop_year = target_date.year
        stop_month = target_date.month
        stop_day = target_date.day + 1

        start_date_and_time = datetime(start_year, start_month, start_day, start_hour, start_minute, start_second, start_millisecond)
        start_dt = start_date_and_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        #print("start date is %s" % start_dt)

        stop_date_and_time = datetime(stop_year, stop_month, stop_day, stop_hour, stop_minute, stop_second, stop_millisecond)
        stop_dt = stop_date_and_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        #print("stop date is %s" % stop_dt)

        results_ebeam_1 = trends_sql.get_events_between_dates(start_dt, stop_dt, 1)
        results_ebeam_2 =  trends_sql.get_events_between_dates(start_dt, stop_dt, 2)

        # open file in write mode
        #with open(r'entries.txt', 'a+') as fp:
        #    for item in results_ebeam_1:

        data_length = len(results_ebeam_1)
        index = 0

        while index < data_length:

            timestamp = results_ebeam_1[index]['DateAndTime']
            beam_status_1 = results_ebeam_1[index]['BEAM_ON']
            beam_status_2 = results_ebeam_2[index]['BEAM_ON']
            print(f"{timestamp}, {beam_status_1}, {beam_status_2}")
            index += 1

                #print(f" Item: {item} is of type {type(item)}")

                # write each item on a new line
                #fp.write("%s\n" % item)

        sys.exit(0)

        #trends_sql.get_beam_data(results)
        #trends_sql.get_batch_data(results)

        target_date = target_date + timedelta(days=1, hours=0)
        i = i + 1

        time.sleep(20)

    # sort the events and then write to csv file
    events.sort(key=operator.itemgetter('timestamp'))

    # Get Product Code for Batches and add to dictionary
    for row in events:

        if "batch" in row:

            batch_number = str(row['batch'])
            product_code = data_sql.get_product_code(batch_number)
            et = row['event_text']
            et = et + " Product Code: " + product_code
            row['event_text'] = et

    # Write all the sorted events into a CSV file
    f = open('jobs.csv', 'w')

    # Write all events to file
    for row in events:

        ts = row['timestamp']
        row_to_write = ts.strftime('%m/%d/%Y %H:%M:%S') + ", " + row['event_text'] + "\n"

        # write a row to the csv file
        f.write(row_to_write)

    # close the file
    f.close()

    # Close the database connections
    trends_sql.sql_close()
    data_sql.sql_close()


if __name__ == '__main__':
    main()



