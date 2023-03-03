""" Connector and methods used to access AWS S3"""

import os
import logging

import pandas as pd
from io import StringIO, BytesIO
import boto3

from tradeETL.common.constants import S3FileTypes
from tradeETL.common.custom_exceptions import WrongFormatException

class S3BucketConnector():
    """
     Class for interacting with s3 buckets
    """ 
    def __init__( self, access_key: str, secret_key: str, endpoint_url: str, bucket: str ):
        """
        Constructor for S3 bucket connector

        Parameters:
        access_key: access key for accessing S3
        secret_key: secret key for accessing S3
        endpoint_url: endpoint url to S3
        bucket: S3 bucket name
        """
        self._logger = logging.getLogger( __name__ )
        self.endpoint_url = endpoint_url
        self.session = boto3.Session( aws_access_key_id=os.environ[access_key],
                                        aws_secret_access_key=os.environ[secret_key])
        print(endpoint_url)
        if endpoint_url == 'https://s3.amazonaws.com':
            self._s3 = self.session.resource( service_name="s3" )
        else:
            self._s3 = self.session.resource( service_name="s3", endpoint_url=endpoint_url )
        self._bucket = self._s3.Bucket( bucket )

    def list_files_in_prefix( self, prefix: str ):
        """
        Listing all files with a prefix on the S3 bucket
        Parameters:
            prefix: prefix on the S3 bucket that should be filtered with

        returns:
            files: list of all the file names containing the prefix in the key
        """
        files = [ obj.key for obj in self._bucket.objects.filter(Prefix=prefix) ]
        return files

    def read_csv_to_df( self, key: str, encoding: str = 'utf-8', sep: str = ',' ):
        """
        Reading a csv file from the s3 bucket and returning a data frame

        Parameters:
        key: key of the file that should be read
        encoding: encoding of the data inside the csv file
        sep: seperator in the csv file

        return:
        data_frame: Pandas dataframe containing the data of the csv file
        """
        self._logger.info( 'Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO( csv_obj )
        data_frame = pd.read_csv( data, sep=sep )
        return data_frame

    def write_df_to_s3( self, data_frame: pd.DataFrame, key: str, file_format: str ):
        """
        Writing a pandas dataframe to the s3 bucket

        supported file formats:
        .csv
        .parquet

        Parameters:
        data_frame: pandas data frame that needs to written into the s3 bucket
        key: target of the saved file
        file_format: format of the saved file
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None

        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)

        self._logger.info('The file fomat %s is not supported to be written to S3 bucket', file_format)
        raise WrongFormatException

    def __put_object( self, out_buffer: StringIO or BytesIO, key: str ):
        """
        Helper function for self.write_df_to_s3()

        parameters:
        out_buffer: StringIO or BytesIO that should be written to the s3 bucket
        key: target key of the saved file
        """

        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(
            Body=out_buffer.getvalue(),
            Key=key
        )
        return True