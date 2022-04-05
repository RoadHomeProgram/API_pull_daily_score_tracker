# API_pull_daily_score_tracker

These scripts include functions to help automate daily score tracking for participants in the IOP. The `main_API_pull_functions.py` file include functions to identify records that have been created or changed in the last 24 hours, make an API call to the redcap server, process the data, plot the individual daily scores, and summarise the output in a csv file.

The `main` function wraps all these functions together into one single call. It's main arguments are `apiToken`, `apiURL` and `root_out` where root out is the base directory where results should be output.

It should be noted for RHP staff to never share API tokens publically, including uploading them to github. If this occurs, please delete or regenerate your API token immediately to maintain security.


Also included in this directory is an example job scheduler compatible with the OS X preferred scheduler `launchd`
