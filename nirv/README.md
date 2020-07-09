Code to generate time series of NIRv at any `lat/lon`, using Google Earth Engine as the backend. 
Currently supports `Landsat 5`, `Landsat 8`, and `MODIS`. 
For the best time, use `MODIS`.

# Running the Code
If all dependencies are installed, you should just need to `cd` to `rubisco-gpp/nirv` and run 
`python get_site_nirv.py`. 
That will go through all sites listed in `data/sites.csv` and generate a MODIS (pseduo-daily) timeseries
of NIRv, outputing the results into `data/nirv/`. 

## Python Environment
I think this thing just needs a few libraries to run, which I've placed in `../requirements.txt`.


## Setting up Earth Engine
You also need an earthengine account to get this to work

1. Install `earthengine-api`:
If you use pip, `pip install earthengine-api` 
or, if you're using conda, `conda install earthengine-api -c conda-forge`.
2. Create Earth Engine Account: 
If you don't already have a GEE account, head on over to https://signup.earthengine.google.com/.
Approval should be immediate.
Pro-tip: if your institution supports unlimited `GoogleDrive`, use your institutional (e.g., GSuite) account.
3. Authenticate:
Open the command line, and from the python environment where you install `earthengine-api`, 
type `earthengine authenticate`.
A link will (should) appear in the terminal (email me if a thing called `Elinks` opens up...). 
Paste that link into your browser of choice and log in with your google account you used above.
Click Allow. 
The next screen should show an authentication token. 
Copy that token and paste it back into the terminal. 
This should store GEE credentials in ~/.config/earthengine, meaning you won't need to repeat this step (often).
More here: https://developers.google.com/earth-engine/python_install_manual#setting-up-authentication-credentials