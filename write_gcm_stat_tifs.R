# setup
library(raster)
library(data.table)
library(dplyr)
library(tidyr)
library(purrr)

load("/atlas_scratch/mfleonawicz/projects/nwt/workspaces/nwt_data.RData")
dir.create(outDir <- "/atlas_scratch/mfleonawicz/projects/nwt/data/tifs/stats", recursive=TRUE, showWarnings=FALSE)

# get cross-GCM statistic rasters and organize in data table
get_stats <- function(x, decades){

    by_var_rcp <- function(x, month, decade.index){
        group_by(d, Var, RCP, Model) %>%
        do(Maps=map(.$Maps[[1]][month], ~subset(.x, decade.index)) %>% brick) %>% group_by(Var, RCP) %>%
        do(Maps=brick(.$Maps)) %>% group_by(Var, RCP) %>%
        do(Min=calc(.$Maps[[1]], min), Mean=calc(.$Maps[[1]], mean), Max=calc(.$Maps[[1]], max)) %>% group_by(Var, RCP)
    }
    
    by_decade <- function(x, decade.index){
        map2_df(month.abb, decade.index, ~by_var_rcp(x, .x, .y), .id="Month") %>%
        mutate(Month=factor(month.abb[as.numeric(Month)], levels=month.abb))
    }
    
    idx <- seq_along(decades)
    map_df(idx, ~by_decade(x, .x), .id="Decade") %>% mutate(Decade=decades[as.numeric(Decade)]) %>%
        group_by(Var, RCP, Decade, Month)
}

decades <- paste0(seq(2010, 2090, by=10), "s")
d.stats <- get_stats(d, decades)

# Additional stat and final organizing
d.mag <- d.stats %>% do(Magnitude=.$Max[[1]]-.$Min[[1]])

d.stats <- lapply(c("Min", "Mean", "Max"), function(i, x){
    mo <- c(paste0(0, 1:9), 10:12)
    select_(x, .dots=list(i)) %>%
    mutate(Stat=i, outfile=paste(c(Var, tolower(i), RCP, Decade, mo[Month]), collapse="_")) %>%
    setnames(c("Var", "RCP", "Decade", "Month", "Map", "Stat", "outfile"))
    }, x=d.stats) %>% bind_rows

d.mag <- mutate(d.mag, Stat="Magnitude",
    outfile=paste(c(Var, "mag", RCP, Decade, c(paste0(0, 1:9), 10:12)[Month]), collapse="_")) %>%
    setnames(c("Var", "RCP", "Decade", "Month", "Map", "Stat", "outfile"))

d.stats <- bind_rows(d.stats, d.mag)

# save tifs
d.stats %>% split(.$outfile) %>% walk(~writeRaster(.$Map[[1]], file.path(outDir, .$outfile), format="GTiff", datatype="FLT4S"))
    