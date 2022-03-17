setwd("~/Documents/transit/")

library(RProtoBuf)
library(data.table)
library(sf)


##########################
####  LIVE FEED DATA  ####
##########################



## load the actual proto file which specifies how this all works
RProtoBuf::readProtoFiles("gtfs-realtime.proto")
## check all the 'Descriptors' that are now available for loading available feeds
ls("RProtoBuf:DescriptorPool")


## read the actual feed
vehicle_position_feed <- read(GTFSv2.Realtime.FeedMessage,  "SEQ-VehiclePositions.pb")[["entity"]]
trip_update_feed  <- read(GTFSv2.Realtime.FeedMessage,  "SEQ-TripUpdates.pb")[["entity"]]


## little function so i can extract nested [[x]][[y]][[z]]
## in a simpler way using a vector of depths c("x","y","z") instead
red_ext <- function(x, data) sapply(data, function(d)  Reduce(`[[`, x, init=d)  )


## reformat to data.tables

## vehicle position
vehicle_position <- as.data.table(Map(
    red_ext,
    list(
        "id"             = c("id"),
        "latitude"       = c("vehicle","position","latitude"),
        "longitude"      = c("vehicle","position","longitude"),
        "timestamp"      = c("vehicle","timestamp"),
        "vehicle_id"     = c("vehicle","vehicle","id"),
        "current_status" = c("vehicle","current_status"),
        "trip_id"        = c("vehicle","trip","trip_id"),
        "route_id"       = c("vehicle","trip","route_id")
    ),
    list(vehicle_position_feed)
))
## timestamp in Brisbane - GMT+10 time (yes, that Etc/GMT-10 is correct)
vehicle_position[, timestamp := as.POSIXct(timestamp, origin="1970-01-01", tz="Etc/GMT-10")]


## trip update
trip_update <- as.data.table(Map(
    red_ext,
    list(
        "trip_id"          = c("trip_update","trip","trip_id"),
        "route_id"         = c("trip_update","trip","route_id"),
        "start_date"       = c("trip_update","trip","start_date"),
        "start_time"       = c("trip_update","trip","start_time"),
        "stop_time_update" = c("trip_update","stop_time_update")
    ),
    list(trip_update_feed)
))
## start_datetime in Brisbane - GMT+10 time (yes, that Etc/GMT-10 is correct)
trip_update[, start_datetime := as.POSIXct(paste(start_date, start_time), format="%Y%m%d %H:%M:%S", tz="Etc/GMT-10")]


## stops table separated out - joins to trip_update table on trip_id/route_id
stops <-cbind(
    trip_update[,c("trip_id","route_id")],
    as.data.table(Map(
        \(pth,data) lapply(data, red_ext, x=pth),
        list(
            "stop_sequence"   = c("stop_sequence"),
            "stop_id"         = c("stop_id"),
            "arrival_time"    = c("arrival","time"),
            "arrival_delay"   = c("arrival","delay"),
            "departure_time"  = c("departure","time"),
            "departure_delay" = c("departure","delay")
        ),
        list(trip_update[["stop_time_update"]])
    ))
)
## unlist to long form so each line is the record of the time/delay
## at each stop for each service
stops <- stops[, lapply(.SD, unlist), by=c("trip_id","route_id")]
## arrival and departure times as proper timestamps
stops[, c("arrival_time","departure_time") := lapply(
            .(arrival_time,departure_time),
            \(x) as.POSIXct(x, origin="1970-01-01", tz="Etc/GMT-10"))
]


## remove stop_time_update from trip_update table now that separated out
trip_update[, stop_time_update := NULL]


#########################
####    META DATA    ####
#########################

## Read in ALL the meta-data to use for making sense of the feed.
## Used read.csv and converted to data.table afterwards instead of
## fread(), just so built-in unzip() could be used directly.
## Files aren't big, so isn't a bottleneck.
meta_lf <- unzip("meta/SEQ_GTFS.zip", list=TRUE)$Name
meta <- setNames(
    lapply(meta_lf, \(x) as.data.table(read.csv(unz("meta/SEQ_GTFS.zip", x)))),
    sub("\\.txt", "", meta_lf)
)


#########################
####   STREET DATA   ####
#########################


## import while filtering to a bounding box
## north Brisbane for an example area
bb <- st_bbox(c(xmin = 152.953006, xmax = 153.075612, ymax = -27.321313, ymin = -27.408082))
bbosm <- st_as_text(st_as_sfc(bb))


## main roads
osm_prhway <- st_read(
    "osm_australia/australia-latest.osm.pbf",
    query = "select * from lines where highway in ('motorway','trunk','primary','trunk_link')",
    wkt_filter = bbosm
)

## all the other roads
osm_secroad <- st_read(
    "osm_australia/australia-latest.osm.pbf",
    query = "select * from lines where not(highway in ('motorway','trunk','primary','trunk_link'))",
    wkt_filter = bbosm
)

## multi-line-strings
osm_mls <- st_read(
    "osm_australia/australia-latest.osm.pbf",
    query = "select * from multilinestrings",
    wkt_filter = bbosm
)
#### subset to translink railway lines only
osm_railline <- osm_mls[grepl('"route"=>"train"', osm_mls$other_tags) &
        grepl('"network"=>"TransLink"', osm_mls$other_tags),]

## points
osm_pts <- st_read(
    "osm_australia/australia-latest.osm.pbf",
    query = "select * from points",
    wkt_filter = bbosm
)
#### subset to railway stations only
osm_railstat <- osm_pts[grepl('"railway"=>"station"', osm_pts$other_tags),]



## plot it all within the bounding box explicitly as some bits 'within' extend outside
plot(osm_secroad["_ogr_geometry_"], col="#cccccc", reset=FALSE, xlim=bb[c(1,3)], ylim=bb[c(2,4)])
plot(osm_prhway["_ogr_geometry_"],  col="#31878e", lwd=2, add=TRUE)
plot(osm_railline["_ogr_geometry_"],  col="#c6a023", lwd=2, add=TRUE)
#### plot station as a double dot
plot(osm_railstat["_ogr_geometry_"],  col="#000000", bg="#ffffff", pch=21, cex=2, lwd=2, add=TRUE)
plot(osm_railstat["_ogr_geometry_"],  col="#000000", bg="#000000", pch=21, cex=0.7, lwd=1, add=TRUE)



#####################################
####   ADD 'LIVE' VEHICLE DATA   ####
#####################################

## everything plotted at once as a simple test
##vp_sf <- st_as_sf(vehicle_position, coords = c("longitude","latitude"), crs = st_crs(osm_prhway))
##plot(vp_sf["geometry"],  col="#000000", bg="#ff0000", pch=21, add=TRUE)

## keys to figure this out

#### FEED DATA
## vehicle_position = id, vehicle_id, trip_id, route_id
## trip_update      = trip_id, route_id
## stops            = trip_id, route_id, stop_id

#### META DATA
## agency           = {basic info on translink, not feed-related}
## calendar_dates   = service_id
## calendar         = service_id
## feed_info        = {basic info on feed}
## routes           = route_id + route_type (4 = bay island ferry, 3 = buses, 2 = trains, 0 = g:link trams)
## shapes           = shape_id + shape_pt_sequence
## stop_times       = trip_id, stop_id + stop_sequence
## stops            = stop_id, zone_id
## trips            = route_id, service_id, trip_id, direction_id, block_id, shape_id

#### RELATIONSHIPS/ADD INFO
## ---------------------------------------------------------------------------------------------------------------
## Description                     Variable                  Join criteria                        
## ---------------------------------------------------------------------------------------------------------------
## Vehicle Type (bus/train etc) :: meta$routes$route_type :: vehicle_position$route_id  <=> meta$routes$route_id
## Route vehicle is on


## plot vehicle positions with vehicle type
vehicle_position_ext <- vehicle_position[meta$routes, on="route_id", route_type := i.route_type]
vehicle_position_ext[, route_type := factor(route_type, levels=c(0,2,3,4), labels=c("g:link","train","bus","bay island ferry"))]

vp_sf <- st_as_sf(vehicle_position_ext, coords = c("longitude","latitude"), crs = st_crs(osm_prhway))
plot(vp_sf["geometry"], col="#000000", bg="#ff0000", pch=c(24,21,22,25)[vp_sf$route_type], add=TRUE)



