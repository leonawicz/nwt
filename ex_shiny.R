library(shiny)
library(shinyBS)
library(raster)
library(data.table)
library(dplyr)
library(leaflet)
library(ggplot2)
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
    checkboxInput("legend", "Show legend", TRUE),
    selectInput("location", "Community", c("", locs$loc), selected="", multiple=F),
    actionButton("button_plot_and_table", "View Plot/Table")
  ),
  bsModal("Plot_and_table", "Plot and Table", "button_plot_and_table", size = "large",
          plotOutput("TestPlot"),
          dataTableOutput("TestTable")
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
      proxy %>% hideGroup("locations") %>% removeMarker(layerId="Selected")
    }
  })

  #add_CM <- function(x, p) addCircleMarkers(p$lng, p$lat, radius=10, color="black", fillColor="orange", fillOpacity=1, opacity=1, stroke=TRUE, layerId="Selected")

  observeEvent(input$Map_marker_click, { # update the map on map clicks
    p <- input$Map_marker_click
    proxy <- leafletProxy("Map")
    if(p$id=="Selected"){
      proxy %>% removeMarker(layerId="Selected")
    } else {
      proxy %>% setView(lng=p$lng, lat=p$lat, input$Map_zoom) %>% addCircleMarkers(p$lng, p$lat, radius=10, color="black", fillColor="orange", fillOpacity=1, opacity=1, stroke=TRUE, layerId="Selected") #add_CM(p)
    }
  })

  observeEvent(input$Map_marker_click, { # update the location selectInput on map clicks
    p <- input$Map_marker_click
    if(!is.null(p$id)){
      if(is.null(input$location) || input$location!=p$id) updateSelectInput(session, "location", selected=p$id)
    }
  })

  observeEvent(input$location, { # update the map on location selectInput changes
    p <- input$Map_marker_click
    p2 <- subset(locs, loc==input$location)
    proxy <- leafletProxy("Map")
    if(nrow(p2)==0){
      proxy %>% removeMarker(layerId="Selected")
    } else if(input$location!=p$id){
      proxy %>% setView(lng=p2$lon, lat=p2$lat, input$Map_zoom) %>% addCircleMarkers(p2$lon, p2$lat, radius=10, color="black", fillColor="orange", fillOpacity=1, opacity=1, stroke=TRUE, layerId="Selected") #add_CM(p2)
    }
  })

  Data <- reactive({ d <- d.cru$Locs[[2]] %>% filter(Location=="Yellowknife" & Month=="Jun") })

  output$TestPlot <- renderPlot({ ggplot(Data(), aes(value, Year)) + geom_line() + geom_smooth() })

  output$TestTable <- renderDataTable({
    Data()
  }, options = list(pageLength=5))

}

shinyApp(ui, server)
