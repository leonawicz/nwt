library(parallel)
library(data.table)
library(raster)
library(dplyr)
library(tidyr)
rasterOptions(chunksize=10e10, maxmemory=10e11)

mainDir <- "/Data/Base_Data/Climate/World/World_10min"
gcmDir <- file.path(mainDir, "projected/AR5_CMIP5_models")
dir.create(outDir <- "/atlas_scratch/mfleonawicz/projects/leaflet_global/workspaces", recursive=TRUE, showWarnings=FALSE)
load(file.path(outDir, "sample_global_locs.RData"))
rcp <- paste0("rcp", c(45, 60, 85))
models <- list.files(file.path(gcmDir, rcp[1]))
vars <- c("pr", "tas")
gcm.years <- 2010:2099
cru.years <- 1950:2013

prep_files <- function(dir, rcp, model, variable){
  list(list.files(file.path(dir, rcp, model, variable), pattern="\\.tif$", full=TRUE))
}

d <- rev(expand.grid(Model=models, RCP=rcp, Var=vars, stringsAsFactors=F)) %>%
  data.table %>% group_by(Var, RCP, Model) %>%
  mutate(Data=purrr::pmap(list(rcp=RCP, model=Model, variable=Var, dir=gcmDir), prep_files))
d <- d %>% split(paste(.$Var, .$RCP)) # for larger data sets, split now

d.cru <- data.table(Var=vars) %>% group_by(Var) %>%
  mutate(Data=purrr::pmap(list(rcp="historical", model="CRU/CRU_TS32", variable=Var, dir=mainDir), prep_files))

prep_data <- function(files, locs=NULL, shp=NULL, years, decades=TRUE, digits=1){
  locs <- data.frame(locs)
  rownames(locs) <- locs$loc
  locs <- select(locs, lon, lat)
  mo <- sapply(strsplit(basename(files), "_"), "[", 7)
  files <- split(files, mo)
  x <- mclapply(files,
                function(x, locs, shp, years, decades, digits){
                  all.years <- gsub(".tif", "", sapply(strsplit(basename(x), "_"), "[", 8))
                  s <- stack(x, quick=TRUE) %>% rotate
                  if(!is.null(shp)) s <- crop(s, shp) %>% mask(shp)
                  names(s) <- all.years
                  s <- subset(s, match(years, all.years))
                  if(!is.null(locs)) d <- data.table(t(raster::extract(s, locs))) %>% setnames(rownames(locs)) %>%
                    mutate(Year=years) %>% melt(id.vars="Year", variable.name="Location")
                  if(is.null(locs)) d <- NULL
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

locs <- distinct(locs, loc)

d.cru <- group_by(d.cru, Var, add=TRUE) %>% mutate(Data=purrr::map(Data, ~prep_data(unlist(.), locs, shp=NULL, cru.years)))
d.cru <- group_by(d.cru, Var, add=TRUE) %>% do(., Locs=purrr::transpose(.$Data[[1]])$Locs)
d.cru$Locs <- lapply(1:nrow(d.cru),
  function(k, x) rbindlist(lapply(1:12,
    function(i, x) x[[i]] %>% mutate(Month=factor(names(x)[i], levels=month.abb)), x=x[[k]])),
    x=d.cru$Locs)
d.cru$Locs <- lapply(d.cru$Locs, function(x) mutate(x, Location=as.character(Location)))

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

for(i in seq_along(d)){
  print(i)
  d[[i]] <- group_by(d[[i]], Model, add=TRUE) %>% mutate(Data=purrr::map(Data, ~prep_data(unlist(.), locs, shp=NULL, gcm.years)))
  gc()
  d[[i]] <- group_by(d[[i]], Var, RCP, Model) %>% do(Maps=purrr::transpose(.$Data[[1]])$Maps, Locs=purrr::transpose(.$Data[[1]])$Locs)
  gc()
  d[[i]] <- mutate(d[[i]], Maps=purrr::map(.$Maps, ~brickList_transpose(.x)))
  gc()
  write_map_subtables(d[[i]], outDir=file.path(outDir, "map_subtables"))
  gc(); gc()
  d[[i]] <- select(d[[i]], -Maps)
  gc()
  d[[i]]$Locs <- lapply(1:nrow(d[[i]]),
    function(k, x) rbindlist(lapply(1:12,
      function(i, x) x[[i]] %>% mutate(Month=factor(names(x)[i], levels=month.abb)),
      x=x[[k]])),
    x=d[[i]]$Locs)
  d[[i]]$Locs <- lapply(d[[i]]$Locs, function(x) mutate(x, Location=as.character(Location)))
  }
d.gcm <- bind_rows(d)
rm(d)
gc()

save(d.gcm, d.cru, file=file.path(outDir, "global_communities.RData"))

