library(parallel)
library(data.table)
library(raster)
library(dplyr)
library(tidyr)
rasterOptions(chunksize=10e10, maxmemory=10e11)

mainDir <- "/Data/Base_Data/Climate/World/World_10min/projected/AR5_CMIP5_models"
outDir <- "/atlas_scratch/mfleonawicz/projects/nwt/workspaces"
load(file.path(outDir, "nwt_locations.RData"))
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

prep_data <- function(files, locs, shp, years, decades=TRUE, digits=1){
  rownames(locs) <- locs$loc
  locs <- select(locs, lon, lat)
  mo <- sapply(strsplit(basename(files), "_"), "[", 7)
  files <- split(files, mo)
  x <- mclapply(files,
                function(x, locs, shp, years, decades, digits){
                  all.years <- gsub(".tif", "", sapply(strsplit(basename(x), "_"), "[", 8))
                  s <- stack(x, quick=TRUE) %>% rotate %>% crop(shp) %>% mask(shp)
                  names(s) <- all.years
                  s <- subset(s, match(years, all.years))
                  d <- data.table(t(raster::extract(s, locs))) %>% setnames(rownames(locs)) %>%
                    mutate(Year=years) %>% melt(id.vars="Year", variable.name="Location")
                  if(decades) s <- stackApply(s, rep(years[years %% 10 == 0], each=10), mean) %>% round(digits)
                  list(Locs=d, Maps=s)
                }, locs=locs, shp=shp, years=years, decades=decades, digits=digits, mc.cores=min(length(files), 32))
  names(x) <- month.abb
  list(x)
}

# hack to deal with specific NT locations issues
nt.na <- match(c("Paulatuk", "Sachs Harbour", "Ulukhaktok"), locs$loc)
locs$lon[nt.na[2:3]] <- locs$lon[nt.na[2:3]] + 0.1666667
locs$lat[nt.na[1]] <- locs$lat[nt.na[1]] - 0.1666667

d <- group_by(d, Model, add=TRUE) %>% mutate(Data=purrr::map(Data, ~prep_data(unlist(.), locs, shp, years, decades=F)))
d <- group_by(d, Model, add=TRUE) %>% do(., Maps=purrr::transpose(.$Data[[1]])$Maps, Locs=purrr::transpose(.$Data[[1]])$Locs)
d$Locs <- lapply(1:nrow(d),
  function(k, x) rbindlist(lapply(1:12,
    function(i, x) x[[i]] %>% mutate(Month=factor(names(x)[i], levels=month.abb)),
    x=x[[k]])),
  x=d3$Locs)

x <- d$Maps[[1]][[1]]
save(d, file=file.path(outDir, "nwt_data.RData"))
save(x, file=file.path(outDir, "nwt_testing_subset.Rdata"))
