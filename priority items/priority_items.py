# -*- coding: utf-8 -*-
"""
A processor for retrieving items that need to be reviewed and categorized. Since there are many items, with different
items being tracked by different clients, it is impossible to review all of them. This processor aims to mark which
ASINS are most important to review each month, based on which ASINS contribute to the top x% of sales.

Of course, each client will want to track their own ASINS as soon as possible. All own items are also prioritized.

All client_ids and sales numbers are randomized and all passwords/locations are removed or altered. This program is
just meant to show the processing effects. I don't want any private information getting leaked.
"""

# %% Imports

# Go to https://visualstudio.microsoft.com/visual-cpp-build-tools/ and download the visual studio c++ build tools for
# analytics (the first option, like 5 gigs) so that snowflake-sqlalchemy will install
# Assuming that snowflake_connector.py has no issues, required package installation should be:
# pip install --upgrade pandas oauth2client gspread xlrd xlsxwriter openpyxl snowflake-connector-python snowflake-sqlalchemy

import os
from typing import List, Set
from datetime import date
import time
from xlrd import XLRDError  # Used in Error handling in the query_db function

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import pandas as pd

import snowflake_connector as sc  # Edge file used to query Snowflake

# %% Constants - Edit as needed, leave None to run as normal

# TODO
# Add constants specifying where to save/read from Needs Reviews and Priority
# Items (Filepath of historical Needs Reviews and Priority items). Ideally this
# should be on a cloud or shared drive and not locally.

# Filename of pre-collected Needs Reviews to process
# TODO: make it automatically switch to 'None' if the filename is not valid
NR_LIST: str = 'Needs Reviews - 03 Mar 2021.xlsx'

# List of priority items run in the past that should be excluded from the current processing
HISTORICAL_EXCLUDE: List[str] = ['Priority Items - 31 Aug 2020.xlsx',
                                 'Priority Items - 30 Nov 2020.xlsx'
                                 ]

# Any particular client_ids that should not be included in the processing
# Please ensure that there is a space between each Client ID
CLIENT_EXCLUDE: List[int] = [None]

# Any client_ids that should be processed exclusively
# Please ensure that there is a space between each Client ID
# TODO
# It does not work in all cases. using [218] selects 218 but 2 other clients as well. Investigate
ONLY_CHECK_CLIENTS: List[int] = [None]

# Incidates to process the top x percent of sales. By default, this is 95%.
# Raise it to get more items, lower it to get fewer
PERCENTAGE_TO_PROCESS: float = 0.95

# Indicator whether or not to post to the Needs Review tracker
POST_TO_TRACKER: bool = False


# %% Connections and setup

def setup_google_sheet(sheet_name: str) -> gspread.models.Spreadsheet:
    """
    Initialize a connection to Google sheets

    Requires credentials to the Service account in 'credentials.json'. Please
    make sure that this file is in the directory that you're running this
    processor from

    Parameters
    ----------
    sheet_name : str
        Name of the Google worksheet to open

    Returns
    -------
    sheet : gspread.models.Spreadsheet
        An open connection to the spreadsheet

    """
    scope = ["https://spreadsheets.google.com/feeds",
             'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "credentials.json",
        scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name)
    return sheet


# This try/except case was added in to make the program work without setting up the Google Sheet connection. Since this
# is just a demo, there is no need to open and read the Needs Review Tracker sheet. The Needs Reviews will just be
# pulled and processed from the Needs Reviews .xlsx file in the local folder.
try:
    needs_review_tracker_sheet = setup_google_sheet("Needs Reviews Tracker")
except ValueError:
    pass


def get_client_list(sheet: gspread.models.Spreadsheet, tab: str) -> Set[str]:
    """
    Retrieve a list of Client IDs to query Snowflake with

    Parameters
    ----------
    sheet : gspread.models.Spreadsheet
    tab : str
        The name of the tab to open on the Needs Review Tracker spreadsheet

    Returns
    -------
    client_list: Set[str]
        A set of all Client IDs labeled 'Active' or 'Active - OCR to Send to
        Client First' found in the 'Client List' tab of the Needs Review
        Tracker. These are returned as integers converted to strings with a
        comma after each ID. This is done to provide an easily readable string
        of Client IDs to query Snowflake with.
        Ex. {'294,', '305,', '442,'}

    """
    cursheet = sheet.worksheet(tab)
    curclients = cursheet.get_all_records()
    curclients = pd.DataFrame(curclients)
    active_clients = curclients[(curclients.Status == 'Active') |
                                (curclients.Status ==
                                 'Active - OCR to Send to Client First')]
    client_list = active_clients['Client ID']
    client_list = {str(x)+',' for x in client_list}
    print('Found ' + str(len(client_list)) + ' active clients in Client List')
    return client_list


def query_db() -> pd.DataFrame:
    """
    Query Snowflake to retrieve the data needed to process Priority items

    Returns
    -------
    needs_reviews: pandas DataFrame
        Data is returned in the format:
            client_id: int
            track_item: str
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review

    """
    client_list: Set[str] = get_client_list(sheet=needs_review_tracker_sheet, tab='Client List')

    formatted_list = client_list
    if None in ONLY_CHECK_CLIENTS:
        formatted_list = str(formatted_list).replace("'", "")
        formatted_list = str(formatted_list).replace(",,", ",")[1:-2]
    elif None not in ONLY_CHECK_CLIENTS:
        formatted_list = str(ONLY_CHECK_CLIENTS)[1:-1]
        print("Checking only clients: " + formatted_list)
    for client_id in CLIENT_EXCLUDE:
        if client_id is not None:
            print("Excluding client " + str(client_id) +
                  " from available Needs Reviews")
            formatted_list = formatted_list.replace(' ' + str(client_id) + ',', '')
            formatted_list = formatted_list.replace(' ,', '')
    assert formatted_list, 'client_id list is invalid. It may be blank. \
                            Check that the CLIENT EXCLUDE and ONLY CHECK \
                            CLIENTS constants do not cancel each other out'
    # TODO
    # Add a case statement to this query where if no Needs Reviews are pulled
    # for an active client (e.g. if a catalog ends up with 12,000 'Needs
    # Review - Variations' in their catalog and none of them have sales)
    # then it will pull all Needs Reviews in that client, up to a limit (say,
    # up to 500 ASINs)
    query = "SELECT RC.client_id, \
                    RC.STATUS track_item, \
                    asin, \
                    sum(zeroifnull(sales)) total_sales \
             FROM MY_DATABASE.MY_SCHEMA.MY_SALES_TABLE MST \
             RIGHT JOIN MY_DATABASE.MY_SCHEMA.RAW_CATALOG RC \
             ON MST.CATALOG_KEY = RC.CATALOG_KEY \
             WHERE RC.CLIENT_ID IN(" + formatted_list + ") \
             AND track_item ILIKE '%need%review%' \
             GROUP BY RC.client_id, asin, track_item \
             HAVING total_sales > 0 \
             OR track_item ILIKE 'Needs Review - Kragle Sales Report' \
             or track_item ILIKE '%vendor%central%'"
    print("Retrieving current Needs Review data from Snowflake...")
    needs_reviews = pd.DataFrame(sc.get_sf_data(query))
    return needs_reviews


def read_needs_reviews(needs_review_list: str = None) -> pd.DataFrame:
    """
    Read a csv or xlsx file containing Needs Review data to be processed

    Parameters
    ----------
    needs_review_list : str
        Filename of a csv of the data to process. This assumes
        that the Priority Items Query has already been run and exported,
        and that the filename you pass in is the exported data.

    Returns
    -------
    needs_reviews: pandas DataFrame
        Returns a DataFrame of the data required to process
        the Priority Items. It returns in the order:
            client_id: int
            track_item: str
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review

    """
    try:
        needs_reviews = pd.read_csv(needs_review_list)
    except (UnicodeDecodeError, XLRDError, pd.errors.ParserError):
        needs_reviews = pd.read_excel(needs_review_list)
    for client_id in CLIENT_EXCLUDE:
        if client_id is not None:
            print("Excluding client " + str(client_id) +
                  " from available Needs Reviews")
            needs_reviews = needs_reviews[needs_reviews['client_id'] != client_id]
    return needs_reviews


def collect_needs_reviews() -> pd.DataFrame:
    """
    Read Needs Review data from a file or query the data if no file is available

    If Needs Reviews are retrieved from Snowflake, an Excel file is created as
    a historical log. This file can be used for future processing or
    reprocessing so that Snowflake does not need to be queried again

    Returns
    -------
    needs_reviews : pandas DataFrame
        Data is returned in the format:
            client_id: int
            track_item: str
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review

    """
    try:
        needs_reviews: pd.DataFrame = read_needs_reviews(NR_LIST)
        print("Needs Reviews have been read from a previously generated file")
    except ValueError:
        needs_reviews: pd.DataFrame = query_db()
        raw_data = pd.ExcelWriter("Needs Reviews - " +
                                  date.today().strftime('%d %b %Y') +
                                  ".xlsx",
                                  engine='xlsxwriter')
        needs_reviews.to_excel(raw_data,
                               sheet_name='Raw Needs Reviews',
                               index=False)
        raw_data.save()
    return needs_reviews


raw_needs_reviews: pd.DataFrame = collect_needs_reviews()

# %% Preprocessing


preprocessed_needs_reviews: pd.DataFrame = raw_needs_reviews


def mark_vc_items(track_item):
    if 'sales report' in track_item.lower() or 'vendor central' in track_item.lower():
        return 'OWN ITEM'
    else:
        return ''


print('Marking Vendor Central items')
preprocessed_needs_reviews['VC Status'] = preprocessed_needs_reviews \
    .track_item.apply(mark_vc_items)


def create_exclusion_list(exclude: List[str] = None) -> pd.DataFrame:
    """
    Create a list of historical Priority Items that should not be processed

    If there is no historical Priority Item files, no ASINs will be excluded
    from the Needs Review pool.

    This function is needed so that we do not mark duplicate ASINs as priority
    between runs.

    Parameters
    ----------
    exclude : List[str], optional
        List of the filenames of all historical Priority Items.
        Data in each file should be in format:
            client_id: int
            asin: str
            VC Status: str

    Returns
    -------
    historical_exclude: pandas Dataframe
        All client-ASIN pairs that should not be included in processing,
        combined into a single dataframe
        Data returned should be in format:
            client_id: int
            asin: str
            VC Status: str
        If there are more columns, the historical Priority Items files do not
        follow this format. Format is case-sensitive. Additional columns
        should not be an issue so long as the Client ID columns is client_id'
        (not 'Client ID')

        TODO: Add a line or two that renames the column to 'client_id' if it
        is not spelled correctly, to make sure that all duplicate Needs
        Reviews are captured

    """
    print("Reading ASINs to exclude")
    exclusion_list = []
    if exclude is not None:
        for exclusion_file in exclude:
            try:
                exclusion = pd.read_excel(os.getcwd() + "\\priority_items_historical\\" + exclusion_file)
            except (UnicodeDecodeError, XLRDError, pd.errors.ParserError, ValueError):
                exclusion = pd.read_csv(os.getcwd() + "\\priority_items_historical\\" + exclusion_file)
            exclusion_list.append(exclusion)
        historical_exclude = pd.concat(exclusion_list)
        return historical_exclude
    else:
        return None


asins_to_exclude: pd.DataFrame = create_exclusion_list(HISTORICAL_EXCLUDE)


def remove_exclusions_from_needs_reviews(pnr: pd.DataFrame, ate: pd.DataFrame) -> pd.DataFrame:
    """
    Remove matching client-ASIN pairs from the available Needs Review pool

    Recreate preprocessed_needs_reviews by filtering to each client's ASINs in
    both preprocessed_needs_reviews and asins_to_exclude, and not including
    any instances where the ASINs match

    The client-by-client code is rewritten so this step can probably be
    rewritten to be faster or cleaner. Maybe combine it with the actual
    processing?

    Another idea is to create this list ahead of time and find a way to add
    the list to the original snowflake query so that duplicate client-ASIN
    pairs are not pulled from Snowflake in the first place

    pnr : pandas DataFrame
        Preprocessed Needs Reviews
        Data should be in format:
            client_id: int
            track_item: str
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review
            VC Status: str
        Created after OWN ITEMs are marked from :raw_needs_reviews:
    ate : pandas DataFrame
        ASINs to Exclude
        Data should be in format:
            client_id: int
            asin: str
            VC Status: str
        Created from :create_exclusion_list: function

    Returns
    -------
    pnr: pandas DataFrame
        Preprocessed Needs Reviews
            These are the same Preprocessed Needs Reviews that were passed in,
            just without any of the same client-ASIN pairs that are found in
            the ASINs to Exclude list
        Data is returned in format:
            client_id: int
            track_item: str
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review
            VC Status: str

    """
    try:
        print("Removing previous priority items from current Needs Review pool")
        client_unique_asins: List[pd.DataFrame] = []
        clients_to_check = set(pnr['client_id'])
        for client in clients_to_check:
            temp_client = pnr[pnr['client_id'] == client]
            temp_exclude = ate[ate['client_id'] == client]
            client_unique_asins \
                .append(temp_client[temp_client.asin
                                    .apply(lambda asin: asin not in temp_exclude.asin.tolist())])
        return pd.concat(client_unique_asins)
    except TypeError:  # If asins_to_exclude is None
        print("No valid historical Priority ASINs found. No ASINs will be \
              filtered out of previous Priority Items datasets")
        return pnr


ready_to_process = remove_exclusions_from_needs_reviews(preprocessed_needs_reviews,
                                                        asins_to_exclude)
ready_to_process.sort_values(by=['client_id', 'total_sales'],
                             ascending=[True, False], inplace=True)
print("Preprocessing complete! Processing from a total of " +
      str(len(ready_to_process)) + " rows.")


# %% Processing


def process_top_percent_sales(asins_to_process: pd.DataFrame,
                              percentage: float = PERCENTAGE_TO_PROCESS) -> pd.DataFrame:
    """
    Process the Needs Reviews to filter to the specified percentage

    Parameters
    ----------
    asins_to_process : pd.DataFrame
        Data must be in format:
            client_id: int
            track_item: str
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review
            VC Status: str
        Created from :remove_exclusions_from_needs_reviews: function
    percentage : float, optional
        The percentage of sales to filter ASINs to per client. Typical value
        of 0.95 means that this function will return all ASINs contributing to
        the top 95% of sales for each client, starting at the highest-selling
        ASIN. The default is PERCENTAGE_TO_PROCESS, outlined in the Constants
        at the top of this priority_items.py file.

    Returns
    -------
    processed_asins_full : TYPE
        Data returned is in format:
            client_id: int
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review
            VC Status: str

    """
    clients_to_check = set(asins_to_process['client_id'])
    testing_processed_asins = []
    for client_id in clients_to_check:
        print("Currently processing client: " + str(client_id))
        temp_df = asins_to_process[asins_to_process['client_id'] == client_id].reset_index()
        temp_df['total_sales'] = temp_df['total_sales'].fillna(0)
        threshold: float = round(sum(temp_df['total_sales']) * percentage, 2)
        temp_df['cumulative_sales'] = temp_df.total_sales.cumsum()
        temp_df['keep'] = temp_df.apply(lambda x: x[6] <= threshold
                                        or x[5] == 'OWN ITEM', axis=1)
        testing_processed_asins.append(temp_df[temp_df.keep.eq(True)])
    processed_asins_full = pd.concat(testing_processed_asins) \
        .filter(items=['client_id', 'asin', 'total_sales', 'VC Status'])
    return processed_asins_full


processed_asins = process_top_percent_sales(ready_to_process)

# %% Post processing - export and posting to tracker


def create_needs_review_tracker_listings(processed_asins_to_post: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the processed ASINs to be used for the Needs Review Tracker

    Parameters
    ----------
    processed_asins_to_post : pd.DataFrame
        Data must be in format:
            client_id: int
            asin: str
            total_sales: float - includes lifetime sales while in Needs Review
            VC Status: str
        Created from :process_top_percent_sales: function

    Returns
    -------
    tracker_listings : pandas DataFrame
        Data is returned in format:
            Date Assigned: date
            Client ID:int
            Potential Sales in Needs Review: float
            Priority Rank: int
            Priority Needs Review Count: int
        These columns correspond to the columns to be inserted into the Needs
        Review Tracker

    """
    tracker_listings = processed_asins_to_post.groupby('client_id').sum()
    tracker_listings['Priority Needs Review Count'] = processed_asins_to_post \
        .groupby('client_id').size()
    tracker_listings['Avg Sales per ASIN'] = tracker_listings['total_sales'] \
        / tracker_listings['Priority Needs Review Count']
    tracker_listings.sort_values(['Avg Sales per ASIN'],
                                 ascending=False, inplace=True)
    tracker_listings.insert(1, 'Priority Rank',
                            range(1, len(tracker_listings) + 1))
    tracker_listings.reset_index(inplace=True)
    tracker_listings.insert(0, 'Date Assigned',
                            date.today().strftime('%m/%d/%Y'))
    tracker_listings.drop(columns=['Avg Sales per ASIN'], inplace=True)
    tracker_listings.rename(columns={'total_sales': 'Potential Sales in Priority Batch',
                                     'client_id': 'Client ID'}, inplace=True)
    return tracker_listings


def update_gsheet_cells(row, cursheet: gspread.Worksheet, line_num) -> None:
    """
    (helper) Paste values found in tracker_listings to the Needs Review Tracker

    Parameters
    ----------
    row : pandas Series
        The current row in the tracker_listings dataframe
    cursheet : gspread.Worksheet
        The current sheet. This should always be 'Needs Review Tracker'
    line_num : TYPE
        The index of the Needs Review Tracker where there is a blank value.
        This is based off of the length of the 'Date Assigned' column of the
        Needs Review Tracker

    Returns
    -------
    None

    """
    line_num += row[3]  # To go to the next row, according to the Priority Rank value
    cursheet.update_cell(line_num, 8, row[0])  # Date Assigned
    cursheet.update_cell(line_num, 10, row[1])  # client_id
    cursheet.update_cell(line_num, 13, row[2])  # total_sales
    cursheet.update_cell(line_num, 14, row[3])  # Priority Rank
    cursheet.update_cell(line_num, 15, row[4])  # Count - Priority Items
    time.sleep(5)  # Necessary, otherwise it'll throw an error from running too fast


def post_to_needs_review_sheet(tracker_listings) -> None:
    """
    Posts tracker_listings data to the Needs Review Tracker

    Parameters
    ----------
    tracker_listings : pandas DataFrame
        Data must be in format:
            Date Assigned: date
            Client ID:int
            Potential Sales in Priority Batch: float
            Priority Rank: int
            Priority Needs Review Count: int
        created in :create_needs_review_tracker_listings: function

    Returns
    -------
    None

    """
    cursheet = needs_review_tracker_sheet.worksheet('Needs Review Tracker')
    print("Currently adding tracker listings to Needs Review Tracker.")
    print(str(len(tracker_listings)) + " rows are being added.")
    tracker_data = pd.DataFrame(cursheet.get_all_records())
    gsheet_first_empty_line = len(tracker_data['Date Assigned']
                                  # +1 needs to be there or it will place the
                                  # values 1 row above the actual first empty line
                                  .replace('', np.NaN).dropna()) + 1
    tracker_listings.apply(update_gsheet_cells,
                           args=(cursheet, gsheet_first_empty_line), axis=1)


if POST_TO_TRACKER:
    data_for_tracker = create_needs_review_tracker_listings(processed_asins)
    post_to_needs_review_sheet(data_for_tracker)


def export_priority_items(asins_to_export: pd.DataFrame) -> None:
    """
    Saves the ready-to-ship processed Priority Items to an excel file

    Parameters
    ----------
    asins_to_export : pd.DataFrame
        The processed ASINs to be exported into an Excel file

    Returns
    -------
    None

    """
    print("Exporting data...")
    asins_to_export.drop(columns='total_sales', inplace=True)
    curday = date.today().strftime('%d %b %Y')
    writer = pd.ExcelWriter('Priority Items - ' + curday + '.xlsx',
                            engine='xlsxwriter')
    asins_to_export.to_excel(writer, sheet_name='Top ' +
                             str(int(round(PERCENTAGE_TO_PROCESS, 2) * 100)) + '%',
                             index=False)
    writer.save()
    print("Export finished with " + str(len(asins_to_export)) + " rows")


export_priority_items(processed_asins)
