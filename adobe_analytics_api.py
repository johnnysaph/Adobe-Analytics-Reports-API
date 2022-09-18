from datetime import timedelta
from datetime import datetime
import aanalytics2 as api2
from copy import deepcopy
from time import sleep
import pandas as pd
import logging
import json

# const
date_format = '%Y-%m-%d'
day = timedelta(days=1)
    
def get_next_level_request(request, breakdowned, item_id, dimension):
    reqObj = api2.RequestCreator(request)
    metric_ids = reqObj.getMetrics()
    filter_id = breakdowned + ":::" + str(item_id)
    for metric_id in metric_ids:
        reqObj.addMetricFilter(metric_id, filter_id)
    reqObj.setDimension(dimension)
    return reqObj.to_dict()
    
def get_next_level_df(analytics, request, limit=30000):
    response = analytics.getReport(request, verbose=True, item_id=True, n_result="inf", limit=limit)
    df = response['data']
    return df
       
def breakdown_df(analytics, df, dimensions, breakdowned, request):
    next_level_dimensions = deepcopy(dimensions)
    if len(dimensions) == 0:
        return df
    else:
        dimension = next_level_dimensions.pop()
        dfs = []
        for row in df.iterrows():
            breakdowned_value = row[1][breakdowned]
            item_id = row[1]['item_id']
            next_level_request = get_next_level_request(request, breakdowned, item_id, dimension)
            #logging.warning("This is breakdowned: {}, dimension: {}, request: {}.".format(breakdowned, dimension, next_level_request))
            next_level_df = get_next_level_df(analytics, next_level_request)
            #if next_level_df.empty:
                #print(row, next_level_request)
            breakdowned_df = breakdown_df(analytics, next_level_df, next_level_dimensions, dimension, next_level_request)
            breakdowned_df[breakdowned] = breakdowned_value
            dfs.append(breakdowned_df)
        if len(dfs) > 0:
            return pd.concat(dfs)
        else:
            return df
       
class AdobeAnalyticsAPI():
    
    def __init__(self, path_to_config, need_name_to_id=True):
    
        # setting connection with api
        api2.importConfigFile(path_to_config)
        login = api2.Login()
        cids = login.getCompanyId()
        cid = cids[0]['globalCompanyId']
        self.analytics = api2.Analytics(cid, retry=3)

        # get all the segments for reporting
        if need_name_to_id:
            self.segments = self.analytics.getSegments()
            self.name_to_id = {row[1]['name']: row[1]['id'] for row in self.segments.iterrows()}
     
    def get_daily_report(self, rsid, date, metrics=[], dimensions=[], segments=[], segment_ids=[]):
    
        reqObj = api2.RequestCreator()
        reqObj.setRSID(rsid)
        # updating request
        start_date = date
        finish_date = datetime.strftime(datetime.strptime(start_date, date_format) + day, date_format)
        dateRange = start_date + 'T00:00:00.000/' + finish_date + 'T00:00:00.000'
        reqObj.addGlobalFilter(dateRange)
        
        # add metrics to request
        for metric in metrics:
            reqObj.addMetric(metric)
        # add segments to request
        
        for segment_name in segments:
            segment_id = self.name_to_id[segment_name]
            reqObj.addGlobalFilter(segment_id)
        for segment_id in segment_ids:
            reqObj.addGlobalFilter(segment_id)
            
        # get the data
        inner_dimensions = deepcopy(dimensions)
        breakdowned = inner_dimensions.pop()
        #logging.warning("This is high level dimension: {}.".format(breakdowned))
        reqObj.setDimension(breakdowned)
        reqObj.setLimit(30000)
        
        # get high level data
        request = reqObj.to_dict()
        logging.warning("This is request: {}.".format(request))
        response = self.analytics.getReport(request, verbose=True, item_id=True, n_result="inf", limit=30000)
        df = response['data']
        logging.warning("The number of high level rows: {}.".format(len(df)))
        if len(inner_dimensions) == 0:
            del df['item_id']
            return df
            
        # get all the breakdowned data
        final_df = breakdown_df(self.analytics, df, inner_dimensions, breakdowned, request)
        del final_df['item_id']
        for metric in metrics:
            copy_metric = metric + '_copy'
            final_df.insert(len(final_df.columns), copy_metric, final_df[metric])
            del final_df[metric]
            final_df.rename(columns={copy_metric: metric}, inplace=True)
        return final_df
        
    def get_report(self, rsid, start_date, finish_date, metrics=[], dimensions=[], segments=[], segment_ids=[]):
        reqObj = api2.RequestCreator()
        reqObj.setRSID(rsid)
        # updating request
        dateRange = start_date + 'T00:00:00.000/' + finish_date + 'T00:00:00.000'
        reqObj.addGlobalFilter(dateRange)
        
        # add metrics to request
        for metric in metrics:
            reqObj.addMetric(metric)
        # add segments to request
        
        for segment_name in segments:
            segment_id = self.name_to_id[segment_name]
            reqObj.addGlobalFilter(segment_id)
        for segment_id in segment_ids:
            reqObj.addGlobalFilter(segment_id)
            
        # get the data
        inner_dimensions = deepcopy(dimensions)
        breakdowned = inner_dimensions.pop()
        logging.warning("This is high level dimension: {}.".format(breakdowned))
        reqObj.setDimension(breakdowned)
        reqObj.setLimit(30000)
        
        # get high level data
        request = reqObj.to_dict()
        logging.warning("This is request: {}.".format(request))
        response = self.analytics.getReport(request, verbose=True, item_id=True, n_result="inf", limit=30000)
        df = response['data']
        logging.warning("The number of high level rows: {}.".format(len(df)))
        if len(inner_dimensions) == 0:
            del df['item_id']
            return df
            
        # get all the breakdowned data
        final_df = breakdown_df(self.analytics, df, inner_dimensions, breakdowned, request)
        del final_df['item_id']
        for metric in metrics:
            copy_metric = metric + '_copy'
            final_df.insert(len(final_df.columns), copy_metric, final_df[metric])
            del final_df[metric]
            final_df.rename(columns={copy_metric: metric}, inplace=True)
        return final_df
