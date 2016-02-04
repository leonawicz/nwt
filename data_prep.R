library(parallel)
library(data.table)
library(raster)
library(dplyr)
library(tidyr)
rasterOptions(chunksize=10e10, maxmemory=10e11)

mainDir <- "/Data/Base_Data/Climate/World/World_10min/projected/AR5_CMIP5_models"
rcp <- paste0("rcp", c(45, 60, 85))
models <- list.files(file.path(mainDir, rcp[1]))
vars <- c("pr", "tas")
years <- 2010:2099

p4string <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
shape.file <- "/atlas_scratch/mfleonawicz/projects/DataExtraction/data/shapefiles/Political/CanadianProvinces_NAD83AlaskaAlbers.shp"
shp <- shapefile(shape.file) %>% subset(NAME=="Northwest Territories") %>% spTransform(CRS(p4string))

prep_files <- function(dir, rcp, model, variable){
  list(list.files(file.path(dir, rcp, model, variable), pattern="\\.tif$", full=TRUE))
}

d <- rev(expand.grid(Model=models, RCP=rcp, Var=vars, stringsAsFactors=F)) %>%
  data.table %>% group_by(Var, RCP, Model) %>%
  mutate(Data=purrr::pmap(list(rcp=RCP, model=Model, variable=Var, dir=mainDir), prep_files))

prep_data <- function(files, shp, years, decades=TRUE, digits=1){
  mo <- sapply(strsplit(basename(files), "_"), "[", 7)
  files <- split(files, mo)
  x <- mclapply(files,
                function(x, shp, years){
                  all.years <- gsub(".tif", "", sapply(strsplit(basename(x), "_"), "[", 8))
                  s <- stack(x, quick=TRUE) %>% rotate %>% crop(shp) %>% mask(shp)
                  names(s) <- all.years
                  s <- subset(s, match(years, all.years))
                  if(decades) s <- stackApply(s, rep(years[years %% 10 == 0], each=10), mean) %>% round(digits)
                  s
                }, shp=shp, years=years, mc.cores=min(length(files), 32))
  names(x) <- month.abb
  list(x)
}

system.time( d <- group_by(d, Model, add=TRUE) %>% mutate(Data=purrr::map(Data, ~prep_data(unlist(.), shp, years))) )
