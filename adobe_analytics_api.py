from kaspersky.vault import save_to_temp_file, delete_temp_file
from kaspersky.paths import aa_api_report_config, aa_api_config_template
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
folder = 'C:\\\\CANScripts\\\\Adobe Analytics API\\\\'

def update_request(request, breakdowned, dimension, item_id):
    next_level_request = deepcopy(request)
    filter_pattern = {
        "id": None,
        "type": "breakdown",
        "dimension": breakdowned,
        "itemId": item_id
    }
    n = len(next_level_request['metricContainer']['metrics'])
    if 'metricFilters' in next_level_request['metricContainer']: # need to find out current max filter_id
        filter_id = max([int(f['id']) for f in next_level_request['metricContainer']['metricFilters']]) + 1
    else:
        filter_id = 0
        next_level_request['metricContainer']['metricFilters'] = []
    for i in range(n): # apply filters to all metrics and add filters to metricFilters array
        filter_obj = deepcopy(filter_pattern)
        filter_obj['id'] = str(filter_id)
        if 'filters' in next_level_request['metricContainer']['metrics'][i]:
            next_level_request['metricContainer']['metrics'][i]['filters'].append(str(filter_id))
        else:
            next_level_request['metricContainer']['metrics'][i]['filters'] = [str(filter_id)]
        next_level_request['metricContainer']['metricFilters'].append(filter_obj)
        filter_id = filter_id + 1
    next_level_request['dimension'] = dimension
    return next_level_request
    
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
            next_level_request = update_request(request, breakdowned, dimension, item_id)
            #logging.warning("This is breakdowned: {}, dimension: {}, request: {}.".format(breakdowned, dimension, next_level_request))
            next_level_df = get_next_level_df(analytics, next_level_request)
            #if next_level_df.empty:
                #print(row, next_level_request)
            breakdowned_df = breakdown_df(analytics, next_level_df, next_level_dimensions, dimension, next_level_request)
            breakdowned_df[breakdowned] = breakdowned_value
            dfs.append(breakdowned_df)
        return pd.concat(dfs)
        
def breakdown_df_new(analytics, df, dimensions, breakdowned, request):
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
            breakdowned_df = breakdown_df_new(analytics, next_level_df, next_level_dimensions, dimension, next_level_request)
            breakdowned_df[breakdowned] = breakdowned_value
            dfs.append(breakdowned_df)
        if len(dfs) > 0:
            return pd.concat(dfs)
        else:
            return df
    
def create_temp_config_obj(aa_api_config_template, path_to_temp_key):
    with open(aa_api_config_template, 'r') as f:
        lines = f.readlines()
    value_to_insert = '"' + path_to_temp_key + '"'
    string = lines[5]
    string = string.replace('""', value_to_insert)
    lines[5] = string
    return ''.join(lines)
    
class AdobeAnalyticsAPI():
    
    def __init__(self, key, need_name_to_id=True):
    
        # saving key in temporary file
        path_to_temp_key = save_to_temp_file(key, folder, '.key')
        
        try:
            # creating temporary config object
            temp_config_obj = create_temp_config_obj(aa_api_config_template, path_to_temp_key)
            # saving temporary config object in temporary file
            path_to_temp_config = save_to_temp_file(temp_config_obj, folder, '.json')
            # setting connection with api
            api2.importConfigFile(path_to_temp_config)
            login = api2.Login()
            cids = login.getCompanyId()
            cid = cids[0]['globalCompanyId']
            self.analytics = api2.Analytics(cid, retry=3)
        except Exception as ex:
            logging.warning('Adobe Analytics Authentication error occurred.')
            logging.warning(ex)
        finally:
            # deleting temporary files
            delete_temp_file(path_to_temp_key)
            if 'path_to_temp_config' in dir():
                delete_temp_file(path_to_temp_config)

        # get all the segments for reporting
        if need_name_to_id:
            self.segments = self.analytics.getSegments()
            self.name_to_id = {row[1]['name']: row[1]['id'] for row in self.segments.iterrows()}
        # path to report config file
        self.path_to_report_config = aa_api_report_config
    
    # deprecated method
    def run_report(self, start_date, finish_date, metrics=[], dimensions=[], segments=[], segment_ids=[], rsid="kaspersky-single-suite"):
    
        # read request body
        with open(self.path_to_report_config, 'r') as f:
            request = json.loads(f.read())
        
        # rsid
        request["rsid"] = rsid
        
        # updating request
        finish_date = datetime.strftime(datetime.strptime(finish_date, date_format) + day, date_format)
        for i in range(len(request['globalFilters'])):
            if request['globalFilters'][i]['type'] == 'dateRange':
                request['globalFilters'][i]['dateRange'] = start_date + 'T00:00:00.000/' + finish_date + 'T00:00:00.000'
                
        # add metrics to request
        request['metricContainer']['metrics'] = [{'columnId': str(i), 'id': metric} for i, metric in enumerate(metrics)]
        
        # add segments to request
        for segment_name in segments:
            segment_id = self.name_to_id[segment_name]
            request['globalFilters'].append({'type': 'segment', 'segmentId': segment_id})
        for segment_id in segment_ids:
            request['globalFilters'].append({'type': 'segment', 'segmentId': segment_id})
            
        # get the data
        inner_dimensions = deepcopy(dimensions)
        breakdowned = inner_dimensions.pop()
        request['dimension'] = breakdowned
        # get high level data
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
        
    def get_daily_report(self, date, metrics=[], dimensions=[], segments=[], segment_ids=[], rsid="kaspersky-single-suite"):
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
        final_df = breakdown_df_new(self.analytics, df, inner_dimensions, breakdowned, request)
        del final_df['item_id']
        for metric in metrics:
            copy_metric = metric + '_copy'
            final_df.insert(len(final_df.columns), copy_metric, final_df[metric])
            del final_df[metric]
            final_df.rename(columns={copy_metric: metric}, inplace=True)
        return final_df
        
    def get_report(self, start_date, finish_date, metrics=[], dimensions=[], segments=[], segment_ids=[], rsid="kaspersky-single-suite"):
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
        final_df = breakdown_df_new(self.analytics, df, inner_dimensions, breakdowned, request)
        del final_df['item_id']
        for metric in metrics:
            copy_metric = metric + '_copy'
            final_df.insert(len(final_df.columns), copy_metric, final_df[metric])
            del final_df[metric]
            final_df.rename(columns={copy_metric: metric}, inplace=True)
        return final_df