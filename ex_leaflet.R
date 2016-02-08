library(raster)
library(leaflet)
load("workspaces/nwt_testing_subset.RData")

r <- subset(x, 1)
pal <- colorNumeric(c("#0C2C84", "#41B6C4", "#FFFFCC"), values(r), na.color="transparent")

leaflet() %>% addTiles() %>% addRasterImage(r, colors=pal, opacity=0.8) %>%
  addLegend(pal=pal, values=values(r), title="Precipitation (mm)")
