# BrazilWindpower_biascorr

These scripts contain the simulation and bias correction of wind power generation in Brazil from MERRA-2 reanalysis wind speed data with different interpolation and bias correction methods.
More info can be found in the preprint "Assessing the Global Wind Atlas and local measurements for bias correction of wind power generation simulated from MERRA-2 in Brazil"
Link arXiv: https://arxiv.org/abs/1904.13083
Link Energy: https://doi.org/10.1016/j.energy.2019.116212

Results are provided for download at https://zenodo.org/record/3460291

The file "RScript.R" contains the main code for simulation and analysis of simulated time series.
In "MERRA_data.R" functions for the download of MERRA-2 reanalysis data are contained.
"INMETDownload.R", "stations_meta_data.csv" and "ONSDownlaod.R" are scripts for downloading wind speed and wind power generation time series.
The ONS data were actually downloaded manually for use in this Script but part of the time series (since 2015) can be downloaded automatically.

To run the code, first enter the paths at the beginning of the script.
