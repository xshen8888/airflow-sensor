# airflow-sensor

Sense the state of an Airflow dag run. 

If in 'success' state, return True.

If not yet in 'success' state, retry until maximum of retries have been reached, and raise an exceptioin.

Add the sensor to a Flyte workflow to let the workflow wait for the Airflow dag to complete.
