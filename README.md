# BrazilWindpower_biascorr

These scripts contain the simulation and bias correction of wind power generation in Brazil from MERRA-2 reanalysis wind speed data with different interpolation and bias correction methods.
More info can be found in the publication "Assessing simulation and bias correction methods for wind power generation in Brazil – can global datasets compete with local measurements?"
Link: to be added

The file "RScript.R" contains the main code for simulation and analysis of simulated time series.
In "MERRA_data.R" functions for the download of MERRA-2 reanalysis data are contained.
"INMETDownload.R" and "ONSDownlaod.R" are scripts for downloading wind speed and wind power generation time series.
The ONS data were actually downloaded manually for use in this Script but part of the time series (since 2015) can be downloaded automatically.

To run the code, first enter the paths at the beginning of the script.
