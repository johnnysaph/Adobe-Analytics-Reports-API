# Adobe Analytics Reports API

This code is designed to get the report with multiple breakdowns from Adobe Analytics API.
Adode Analytics has a nested data model and it is not so easy to create the report with [multiple dimensions in Adobe Analytics](https://github.com/AdobeDocs/analytics-2.0-apis/blob/master/reporting-multiple-breakdowns.md).
To get the flat table with multiple dimensions just call _get_report_ or _get_daily_report_ methods.

## Dependensies

* [adobe-analytics-api-2.0 library](https://github.com/pitchmuc/adobe-analytics-api-2.0).
* [pandas](https://pandas.pydata.org/)

## Examples
```
import AdobeAnalyticsAPI
path_to_config = <path to json config file>

rsid = <report suite id>
dimensions = ["variables/marketingchannel", "variables/mobiledevicetype"]
metrics = ["metrics/orders"]

analytics = AdobeAnalyticsAPI(path_to_config, need_name_to_id=False)
current_date = "2022-09-18"
daily_df = analytics.get_daily_report(rsid, current_date, metrics=metrics, dimensions=dimensions)
daily_df.head()
```

## List of dimensions

Please find the full list of dimensions [here](https://github.com/AdobeDocs/analytics-1.4-apis/blob/master/docs/reporting-api/elements.md).

## List of metrics

Please find the full list of metrics [here](https://github.com/AdobeDocs/analytics-1.4-apis/blob/master/docs/reporting-api/metrics.md).







