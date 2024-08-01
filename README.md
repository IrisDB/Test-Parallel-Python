# Test Parallel Python

MoveApps

Github repository: *https://github.com/IrisDB/Test-Parallel-Python*

## Description
*Enter here the short description of the App that might also be used when filling out the description during App submission to MoveApps. This text is directly presented to Users that look through the list of Apps when compiling Workflows.*

## Documentation
*Enter here a detailed description of your App. What is it intended to be used for. Which steps of analyses are performed and how. Please be explicit about any detail that is important for use and understanding of the App and its outcomes. You might also refer to the sections below.*

### Application scope
#### Generality of App usability
This App was developed using the example data file `./resources/samples/input2_LatLon.pickle`

#### Required data properties
The App should work for any kind of location data containing multiple individuals.

### Input type
`MovingPandas.TrajectoryCollection`

### Output type
`MovingPandas.TrajectoryCollection`

### Artefacts
*If the App creates artefacts (e.g. csv, pdf, jpeg, shapefiles, etc), please list them here and describe each.*

*Example:* `rest_overview.csv`: csv-file with Table of all rest site properties

### Settings 
None

### Changes in output data
*Specify here how and if the App modifies the input data. Describe clearly what e.g. each additional column means.*

*Examples:*

The App adds to the input data the columns `Max_dist` and `Avg_dist`. They contain the maximum distance to the provided focal location and the average distance to it over all locations. 

The App filterers the input data as selected by the user. 

The output data is the outcome of the model applied to the input data. 

The input data remains unchanged.

### Most common errors
None

### Null or error handling
Not applicable