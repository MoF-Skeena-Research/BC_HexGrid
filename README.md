# BC_HexGrid
Scripts to create hex grids, tile them, and predict from climate data

## Creation of Database

### Create Grid and Send to ClimateBC

Start with the script **MakeGrd.R**. This script reads in a provincial outline, creates a 400m hex grid over it, and then splits the hex grid into tiles. The script then loops through each tile, joins it with a current BGC layer, and extracts elevation from a DEM. It then converts it into a ClimateBC compatible format and writes each tile as a csv labelled Tile*i*_in.csv. The second part of the script is setup to pull data directly from the ClimateBC api, but it may not work properly as I haven't fully tested it yet due to throttling on the API. Until the api is working, it will be necessary to use the desktop app to pull model data for each tile.

### Predict BGCs

Next, use the script **RunPredictHex.R** to predict and send to the database. The script first defines a function for adding variable, and a wrapper function for predicting if the data is too big to fit in memory, and reads in the RandomForest model. It then loops through each tile, reads in the climateBC data, predicts BGC using the model, and formats, and writes the tile to the postgres database. It deletes everything at the end of each tile to free up memory. At the end of this loop, all the predictions should be in the database. For many of our functions, it is still necessary to join the predictions with the spatial hexes and districs etc. This is easiest to do in Postgres.

## Using Database

### Create Map

Once the postgres db has our predictions, we can then create province wide maps using the script **CreateBCMap.R**. This script first defines a function for cleaning crumbs < a specified number of hex cells. The function is fast, but currently doesn't always work perfectly (i.e. it misses some singular cells). Then, the script has two loops, one for future maps, and one for historical maps. The first loop for future maps loops through each specified gcm, and creates the map by district. Once each district is read in, it tiles the district into 16 tiles to allow for parallel dissolving and cleaning. At the end it remerges the tiles and the districtions to give a full map. The historical map loop is much the same. On my machine, each province wide map takes about 15 minutes. Note that it would be straightforward to adapt this script to only create maps for a specified district(s).