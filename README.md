## Lindani Submission

Before executing the python scripts, please run `pip install -r requirements` to install all the necessary packages.

To run the respective tasks, use the command `python tasks{1,2,5}.py`

### Question 2

The threshold value is chosen to balance the tradeoff between the risk of missing critical data and the risk of wasting 
computational resources on problematic data. The optimal threshold will depend on the specific use case and data.

### Question 5
We remove columns that could potentially identify the resident who made the service request.
This anonymization method preserves location accuracy to within approximately 500m,
which should still be useful for spatial analysis while protecting the privacy of the residents who made the request.
Additionally, temporal accuracy is preserved to within 6 hours, which should still be useful for time-based analysis.

