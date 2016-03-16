library(parallel)
library(data.table)
library(raster)
library(dplyr)
library(tidyr)
rasterOptions(chunksize=10e10, maxmemory=10e11)

mainDir <- "/Data/Base_Data/Climate/World/World_10min"
gcmDir <- file.path(mainDir, "projected/AR5_CMIP5_models")
outDir <- "/atlas_scratch/mfleonawicz/projects/nwt/workspaces"
load(file.path(outDir, "nwt_locations.RData"))
rcp <- paste0("rcp", c(45, 60, 85))
models <- list.files(file.path(gcmDir, rcp[1]))
vars <- c("pr", "tas")
gcm.years <- 2010:2099
cru.years <- 1950:2013

p4string <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
shape.file <- "/atlas_scratch/mfleonawicz/projects/DataExtraction/data/shapefiles/Political/CanadianProvinces_NAD83AlaskaAlbers.shp"
shp <- shapefile(shape.file) %>% subset(NAME=="Northwest Territories") %>% spTransform(CRS(p4string))

prep_files <- function(dir, rcp, model, variable){
  list(list.files(file.path(dir, rcp, model, variable), pattern="\\.tif$", full=TRUE))
}

d <- rev(expand.grid(Model=models, RCP=rcp, Var=vars, stringsAsFactors=F)) %>%
  data.table %>% group_by(Var, RCP, Model) %>%
  mutate(Data=purrr::pmap(list(rcp=RCP, model=Model, variable=Var, dir=gcmDir), prep_files))

d.cru <- data.table(Var=vars) %>% group_by(Var) %>%
  mutate(Data=purrr::pmap(list(rcp="historical", model="CRU/CRU_TS32", variable=Var, dir=mainDir), prep_files))

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

brickList_transpose <- function(x){
  id <- names(x)
  idx <- 1:nlayers(x[[1]])
  break_brick <- function(x, y){
    x <- lapply(y, function(i, x) subset(x, i), x=x)
    names(x) <- unlist(purrr::map(x, ~names(.x)))
    x
  }
  x %>% purrr::map2(purrr::map(seq_along(x), ~idx), ~break_brick(.x, .y)) %>%
    purrr::transpose() %>% purrr::map(~brick(.x)) %>% purrr::map(~setNames(.x, id))
}

# hack to deal with specific NT locations issues
nt.na <- match(c("Paulatuk", "Sachs Harbour", "Ulukhaktok"), locs$loc)
locs$lon[nt.na[2:3]] <- locs$lon[nt.na[2:3]] + 0.1666667
locs$lat[nt.na[1]] <- locs$lat[nt.na[1]] - 0.1666667

d.cru <- group_by(d.cru, Var, add=TRUE) %>% mutate(Data=purrr::map(Data, ~prep_data(unlist(.), locs, shp, cru.years)))
d.cru <- group_by(d.cru, Var, add=TRUE) %>% do(., Locs=purrr::transpose(.$Data[[1]])$Locs)
d.cru$Locs <- lapply(1:nrow(d.cru),
  function(k, x) rbindlist(lapply(1:12,
    function(i, x) x[[i]] %>% mutate(Month=factor(names(x)[i], levels=month.abb)),
    x=x[[k]])),
  x=d.cru$Locs)
d.cru$Locs <- lapply(d.cru$Locs, function(x) mutate(x, Location=as.character(Location)))

d <- group_by(d, Model, add=TRUE) %>% mutate(Data=purrr::map(Data, ~prep_data(unlist(.), locs, shp, gcm.years)))
d <- group_by(d, Model, add=TRUE) %>% do(., Maps=purrr::transpose(.$Data[[1]])$Maps, Locs=purrr::transpose(.$Data[[1]])$Locs) %>%
  mutate(Maps=purrr::map(.$Maps, ~brickList_transpose(.x)))

d$Locs <- lapply(1:nrow(d),
  function(k, x) rbindlist(lapply(1:12,
    function(i, x) x[[i]] %>% mutate(Month=factor(names(x)[i], levels=month.abb)),
    x=x[[k]])),
  x=d$Locs)
d$Locs <- lapply(d$Locs, function(x) mutate(x, Location=as.character(Location)))

write_map_subtables <- function(x, decades=seq(2010, 2090, by=10), outDir){
  dir.create(outDir, showWarnings=FALSE)
  do_write <- function(x){
    d <- x
    save(d, file=paste0(outDir, "/", unique(d$Var), "_", unique(d$RCP), "_", unique(d$Decade), ".RData"))
  }
  x <- purrr::map(seq_along(decades),
    ~select(x, -Locs) %>% group_by(Var, RCP, Model) %>% mutate(Decade=decades[.x], Maps=purrr::map2(rep(.x, length(decades)), .$Maps, ~list(.y[.x])))
  )
  x %>% purrr::walk(~.x %>% split(paste(.$Var, .$RCP)) %>% purrr::walk(~do_write(.x)))
}

write_map_subtables(d, outDir=file.path(outDir, "map_subtables"))

x <- d$Maps[[1]][[1]]
d.gcm <- select(d, -Maps)
save(d.gcm, d.cru, file=file.path(outDir, "nwt_communities.RData"))
save(x, d.cru, file=file.path(outDir, "nwt_testing_subset.RData"))
