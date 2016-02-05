library(shiny)
library(raster)
library(dplyr)
library(leaflet)
load("workspaces/nwt_testing_subset.RData")
load("workspaces/nwt_locations.Rdata")

r <- subset(x, 1)
lon <- (xmin(r)+xmax(r))/2
lat <- (ymin(r)+ymax(r))/2
decades <- seq(2010, 2090, by=10)
decade.names <- paste0(decades, "s")
pal <- colorNumeric(c("#0C2C84", "#41B6C4", "#FFFFCC"), values(r), na.color="transparent")

ui <- bootstrapPage(
  tags$style(type="text/css", "html, body {width:100%;height:100%}"),
  leafletOutput("Map", width="100%", height="100%"),
  absolutePanel(top=10, right=10,
    sliderInput("dec", "Decade", min=min(decades), max=max(decades), value=decades[1], step=10, sep="", post="s"),
    checkboxInput("show_communities", "Show communities", TRUE),
    checkboxInput("legend", "Show legend", TRUE)
  )
)

server <- function(input, output, session) {

  ras <- reactive({
    subset(x, which(decades==input$dec))
  })

  ras_vals <- reactive({ values(ras()) })

  output$Map <- renderLeaflet({
    leaflet() %>% setView(lon, lat, 4) %>% addTiles() %>%
      addCircleMarkers(data=locs, radius = ~10, color= ~"#000000", stroke=FALSE, fillOpacity=0.5, group="locations", layerId = ~loc)
  })

  observe({
    proxy <- leafletProxy("Map")
    proxy %>% removeTiles(layerId="rasimg") %>% addRasterImage(ras(), colors=pal, opacity=0.8, layerId="rasimg")
  })

  observe({
    proxy <- leafletProxy("Map")
    proxy %>% clearControls()
    if (input$legend) {
      proxy %>% addLegend(position="bottomright", pal=pal, values=ras_vals(), title="Precipitation", labFormat=labelFormat(suffix="mm"))
    }
  })

  observe({
    proxy <- leafletProxy("Map")
    if (input$show_communities) {
      proxy %>% showGroup("locations")
    } else {
      proxy %>% hideGroup("locations")
    }
  })

}

shinyApp(ui, server)
