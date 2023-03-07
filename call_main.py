
from dotenv import load_dotenv
import os
import main_API_excel_output

load_dotenv()

main_API_excel_output.main(apiToken=os.environ.get('token'),
                           apiURL='https://redcap.rush.edu/redcap/api/',
                           root_out="/Users/rschuber/Library/CloudStorage/OneDrive-rush.edu/score_tracking/"
                           )
