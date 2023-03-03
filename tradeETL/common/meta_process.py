"""
Methods for processing the meta file
"""
from collections import Counter
from datetime import datetime, timedelta
import pandas as pd


from tradeETL.common.s3 import S3BucketConnector

from tradeETL.common.custom_exceptions import WrongMetaFileException
from tradeETL.common.constants import MetaProcessFormat

class MetaProcess():
    """
    class for working with the meta file
    """
    @staticmethod
    def update_meta_file( extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector ):
        """
        Updating the meta file with the processed Trade dates and todays daye as processed date

        Prameters:
        extract_date_list: a list of dates that are extracted from the source
        meta_key: key of the meta file on the S3 bucket
        s3_bucket_data: S3BucketConnector for the bucket with the meta file
        """
        # Creating an empty DataFrame using the meta file column names
        df_new = pd.DataFrame( columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value
        ])
        
        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list

        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)

        try:
            # If meta file exists --> uniion DataFrame of old and new meta data is created
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if Counter(df_old.columns) != Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])

        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta File exists --> only new data is used
            df_all = df_new
        # Writing to s3
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list( first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates based on the input first_date and the already 
        processed dates in the meta file

        first_date: the earliest date . ie etl data should be processed
        meta_key: key of the meta file on the s3 bucket
        s3_bucket_meta: S3BucketConnector for the bucket with meta file


        returns:
        min_date: first date that should be processed
        return_date_list: list of all dates from the min_date till today
        """

        start = datetime.strptime( first_date, 
                                    MetaProcessFormat.META_DATE_FORMAT.value)\
                                        .date() - timedelta(days=1)
        today = datetime.today().date()

        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            df_meta = s3_bucket_meta.read_csv_to_df( meta_key )

            # Creating a list of dates from first_date intill today
            dates = [ start + timedelta(days=x) for x in range(
            0, ( today - start ).days + 1) ]

            # creating set of all dates in meta file
            src_dates = set(pd.to_datetime( df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value] ).dt.date)
            
            dates_missing = set(dates[1:]) - src_dates

            if dates_missing:
                # Determining the earliest date that should be extracted
                min_date = min( set(dates[1:]) - src_dates ) - timedelta(days=1)

                # Creating a list of dates from min_date untill today
                return_min_date = (min_date + timedelta(days=1)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) for date in dates if date >= min_date]
            else:
                # Setting value for the earliest date and list of dates
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = []
        except s3_bucket_meta.session.client( 's3' ).exceptions.NoSuchKey:
            return_min_date = first_date
            return_dates = [ ( start + timedelta(days=x) ).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range(
                0, (today-start).days + 1) ]
            
        return return_min_date, return_dates
        