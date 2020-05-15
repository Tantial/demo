# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 13:39:11 2019

@author: eliasdeadman

This script uses a Google service account to edit a Google spreadsheet. If you
have the proper credentials, go to console.cloud.google.com to work with the
API project settings

Some of the information in this script uses private information. To maintain
privacy, anything I deemed as a potential risk in leaking private information
has been replaced with "[REDACTED]". Thank you for your understanding.

"""
# %%
from datetime import date
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import pandas as pd
import mysql_connector as msc  # Private library to query database


# %%
class PriorityItems():
    """
    Purpose is to create the priority items list from start to finish.

    Since we have more item_ids than a person or team realistically has
    time to categorize, we must choose which item_ids will be the most impactful,
    labeled as Priority Items. Priority Items are item_ids that meet a certain
    sales threshold (top 95% selling items based on sales) or are VC Items, described below.

    Process:
        Establish database connection
        Collect the list of active clients for it to use
        Run the query on the MySQL connection
        Create a pandas dataframe out of it
        Take that new dataframe and add a column to mark the VC items
        Have it calculate the top 95% for each client id
        Create the Needs Review Tracker listings
        Calculate and write the relevant information onto the Needs Review Tracker
        Remove the Track Item status and total sales from Priority Items and Total Sales
        Export the list of priority items, total sales items, and Needs Review
            tracker listings into a single Excel file
    """

    def __init__(self, nr_list: str = None) -> None:
        """
        Runs through the process listed above.

        Parameters
        ----------
        nr_list : str
            Filename of the Needs Review List. This should be a .csv or .xlsx file.
            When running this script, if a file is given, it will use the file
            as a data reference to process. Otherwise, it will query the database
            directly and retrieve the information itself.

        Returns
        -------
        None

        """
        self.top_95_percent: pd.DataFrame
        self.tracker_data: pd.DataFrame
        self.setup_google_sheet()
        self.get_client_list()
        try:
            self.needs_reviews: pd.DataFrame = self.read_needs_reviews(nr_list)
        except ValueError:
            self.needs_reviews: pd.DataFrame = self.query_db()
            raw_data = pd.ExcelWriter("Needs Reviews - " + date.today().strftime('%d %b %Y')
                                      + ".xlsx",
                                      engine='xlsxwriter')
            self.needs_reviews.to_excel(raw_data, sheet_name='Raw Needs Reviews', index=False)
            raw_data.save()
        self.add_vc_items()
        self.process_top_95_percent()
        self.create_nr_tracker_listings()  # Comment this line out if you want to skip posting on the Needs Review tracker
        self.export_to_excel()

    def setup_google_sheet(self) -> None:
        """
        Establishes connection to the Needs Review Tracker. This is needed to
        retreive a list of active clients, and to write the required
        information for tracking.

        The gsheet library ignores hidden or filtered rows, so no prior setup
        is required

        Returns
        -------
        None

        """
        scope = ["https://spreadsheets.google.com/feeds",
                 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "credentials.json",  # Credentials not included for privacy
            scope)
        client = gspread.authorize(creds)
        self.sheet = client.open("Needs Reviews Tracker")

    def get_client_list(self) -> None:
        """
        Reads the rows of all active clients in the Needs Review Tracker. From
        those rows, generates a list of all Client IDs and prepares a list
        that will be used by the SQL Query.

        Returns
        -------
        None

        """
        cursheet = self.sheet.worksheet('Client List')
        curclients = cursheet.get_all_records()
        curclients = pd.DataFrame(curclients)
        self.active_clients = curclients[(curclients.Status == 'Active') |
                                         (curclients.Status ==
                                          'Active - [REDACTED]')]
        self.client_list = self.active_clients['Client ID']
        self.client_list = [str(x)+',' for x in self.client_list]

    def query_db(self) -> pd.DataFrame:
        """
        Runs the query. Uses a list of client ids created in the `get_client_list` method above

        *Note - The query listed below does not originate from me

        Returns
        -------
        pandas DataFrame
            Returns a DataFrame of the data required to process
            the Priority Items. It returns in the order:
                client_id
                track_item
                asin
                total_sales

        """
        formatted_list = self.client_list
        formatted_list = str(formatted_list).replace("'", "")
        formatted_list = str(formatted_list).replace(",,", ",")[1:-2]
        query = 'SELECT ccd.client_id, \
                        ccd.track_item, \
                        ccd.asin, \
                        COALESCE(fcd.sales,0) + \
                        COALESCE(fcd.third_party_ordered_sales,0) \
                        "total_sales" \
                FROM [REDACTED] ccd \
                JOIN [REDACTED] fcd \
                ON ccd.asin = fcd.asin AND ccd.client_id = fcd.client_id \
                JOIN (SELECT client_id, MAX(week_beginning) AS max_week_beginning \
                      FROM [REDACTED] \
                      GROUP BY client_id) result \
                      ON fcd.client_id = result.client_id \
                      AND fcd.week_beginning = result.max_week_beginning \
                      WHERE ccd.client_id IN(' + formatted_list + ') \
                      AND ccd.track_item LIKE "needs review%" \
                HAVING total_sales > 0 OR track_item = "Needs Review - [REDACTED]";'
        print("Retrieving current Needs Review data...")
        raw_data = pd.DataFrame(msc.query_db1(query))
        writer = pd.ExcelWriter('Needs Reviews - ' + date.today().strftime('%d %b %Y') +
                                '.xlsx', engine='xlsxwriter')
        raw_data.to_excel(writer, sheet_name='Needs Reviews', index=False)
        writer.save()
        return raw_data

    def read_needs_reviews(self, needs_review_list: str) -> pd.DataFrame:
        """
        Parameters
        ----------
        needs_review_list : str
            DESCRIPTION. Filename of a csv or xlsx of the data to process. This assumes
            that the Priority Items Query has already been run and exported,
            and that the filename you pass in is the exported data.

        Returns
        -------
        pandas DataFrame
            Returns a DataFrame of the data required to process
            the Priority Items. It returns in the order:
                client_id
                track_item
                asin
                total_sales

        """
        try:
            needs_reviews = pd.read_csv(needs_review_list)
        except UnicodeDecodeError:
            needs_reviews = pd.read_excel(needs_review_list)
        return needs_reviews

    def add_vc_items(self) -> None:
        """
        Checks the Needs Review dataframe from `query_db` for all items marked
        as 'Needs Review - [REDACTED]'. Adds a new column where
        items under 'Needs Review - [REDACTED]' are marked 'VC ITEM'
        in the new column.

        Returns
        -------
        None

        """
        self.needs_reviews['VC Status'] = ['VC ITEM'
                                           if '[REDACTED]' in x.lower()
                                           else ''
                                           for x in self.needs_reviews['track_item']]
        self.total_vc_items = self.needs_reviews[self.needs_reviews['VC Status'] == 'VC ITEM']

    def process_top_95_percent(self) -> None:
        """
        Checks the dataframe and returns a new dataframe with all VC Items and
        the top 95% of sales by client id

        This copies a similar process to Excel's pivot table process, which
        takes 95% of the total sales, searches each item_id starting from the
        highest value, and fills it in up until it reaches that 95%.

        Returns
        -------
        None

        """
        clients_to_check = set(self.needs_reviews['client_id'])
        self.top_95_percent = pd.DataFrame()
        for client in clients_to_check:
            print("Currently processing client:" + str(client))
            temp_df = self.needs_reviews[self.needs_reviews['client_id'] == client]
            temp_df = temp_df.sort_values(by='total_sales', ascending=False)
            working_client = temp_df.where(temp_df['VC Status'] == 'VC ITEM')
            threshold = sum(temp_df['total_sales'])*0.95
            t_95 = 0
            for row in temp_df.iterrows():
                # When iterrating through the rows, it reads the format [index, [values]]
                # so row gets reassigned to the actual values instead of a nested list
                row = row[1]
                new_sales = row[3]
                working_client = working_client.append(row)
                t_95 = t_95+new_sales
                if t_95 >= threshold:
                    break
            working_client = working_client.drop_duplicates()
            working_client = working_client.dropna()
            self.top_95_percent = self.top_95_percent.append(working_client)

    def t_process_top_95_percent(self) -> None:
        """
        Checks the dataframe and returns a new dataframe with all VC Items and
        the top 95% of sales by client id

        This method is used for testing since it's faster than the correct way.
        Instead of filling the top 95% of sales, it checks through the top 95%
        of items.

        Returns
        -------
        None

        """
        clients_to_check = set(self.needs_reviews['client_id'])
        self.top_95_percent = pd.DataFrame()
        for client in clients_to_check:
            print("Currently processing client:" + str(client))
            temp_df = self.needs_reviews[self.needs_reviews['client_id'] == client]
            temp_df = temp_df.sort_values(by='total_sales', ascending=False)
            threshold = temp_df['total_sales'].quantile(0.95)
            working_client = temp_df.where(
                np.logical_or(temp_df['total_sales'] >= threshold,
                              temp_df['VC Status'] == 'VC ITEM'))
            working_client = working_client.dropna()
            self.top_95_percent = self.top_95_percent.append(working_client)

    def add_tracker_listings_to_tracker(self) -> None:
        """
        Reads all information needed for updating the Needs Review Tracker and
        inserts the information at the bottom.

        A new, temporoary dataframe is required so that it uses the correct length
        to determine the location of the row to insert on

        Each row requires a 5 second sleep period. This is because the google
        API only allows 100 requests in a 100 second timespan per user. If this
        limit is exceeded, an error is thrown and the program crashes.

        Returns
        -------
        None
            Output order needs to be:
                Date Assigned (curdate) - Col H
                Client ID - Col J
                Potential Sales - Col M
                Priority Rank - Col N
                Priority Needs Review Count - Col O
            'Col' refers to the destination column on the Needs Review Tracker

        """
        cursheet = self.sheet.worksheet('Needs Review Tracker')
        print("Currently adding tracker listings to Needs Review Tracker")
        self.tracker_data = pd.DataFrame(cursheet.get_all_records())
        temp_tracker_data = self.tracker_data['Date Assigned'].replace('', np.NaN).dropna()
        for num, row in self.tracker_listings.iterrows():
            cursheet.update_cell(len(temp_tracker_data)+num+2, 8, row[0])  # Date Assigned
            cursheet.update_cell(len(temp_tracker_data)+num+2, 10, row[1])  # client_id
            cursheet.update_cell(len(temp_tracker_data)+num+2, 13, row[2])  # total_sales
            cursheet.update_cell(len(temp_tracker_data)+num+2, 14, row[3])  # Priority Rank
            cursheet.update_cell(len(temp_tracker_data)+num+2, 15, row[4])  # Count - Priority Items
            time.sleep(5)  # See docstring for reason why this is here

    def create_nr_tracker_listings(self) -> None:
        """
        Reads [client_id, total_sales(potential sales), priority rank, and
        count of priority items] from the top_95_percent list and creates a new
        dataframe in the right order to copy and paste into the Needs Review
        Tracker

        Returns
        -------
        None
            Output order needs to be:
                Date Assigned (curdate) - Col H
                Client ID - Col J
                Potential Sales - Col M
                Priority Rank - Col N
                Priority Needs Review Count - Col O
            'Col' refers to the destination column on the Needs Review Tracker

        """
        self.tracker_listings = self.top_95_percent.groupby('client_id').sum()
        self.tracker_listings = self.tracker_listings.sort_values(['total_sales'], ascending=False)
        self.tracker_listings['Priority Rank'] = range(1, len(self.tracker_listings) + 1)
        self.tracker_listings['Priority Needs Review Count'] = self.top_95_percent \
                                                                   .groupby('client_id') \
                                                                   .size()
        self.tracker_listings = self.tracker_listings.reset_index()
        self.tracker_listings.insert(0, 'Date Assigned', date.today().strftime('%m/%d/%Y'))
        self.tracker_listings = self.tracker_listings.rename(
            columns={'client_id': 'Client ID',
                     'total_sales': 'Potential Sales within Priority Needs Reviews'})
        self.add_tracker_listings_to_tracker()

    def export_to_excel(self) -> None:
        """
        Removes the 'track_status' and 'total_sales' columns from the priority
        items and total sales sheets.

        Returns
        -------
        None
            Exports an Excel workbook with the first sheet having the priority
            items, and the second sheet having all items with sales or from VC

        """
        print("Exporting data...")
        self.full_data = self.needs_reviews.drop(columns=['track_item', 'total_sales'])
        self.top_95_percent = self.top_95_percent.drop(columns=['track_item', 'total_sales'])
        curday = date.today().strftime('%d %b %Y')
        # Disables pylint's error about pandas ExcelWriter abstract methods
        # pylint: disable=E0110
        writer = pd.ExcelWriter('Priority Items - ' + curday + '.xlsx', engine='xlsxwriter')
        self.top_95_percent.to_excel(writer, sheet_name='Top 95%', index=False)
        self.full_data.to_excel(writer, sheet_name='Full List', index=False)
        writer.save()
        print("Export Finished")


# %%
PriorityItems()
