package ca.mcit.bigdata.course3project.enrichment

import java.nio.file.Paths
import org.apache.hadoop.fs.Path

import scala.io.Source
import ca.mcit.bigdata.course3project.schema.{Calendar, Route, Trip}

object ProjectInit extends ProjectClient with App {

  val path = Paths.get(".").toAbsolutePath
  val distPath = "/user/bdss2001/carlafranca/course3"
  val filesPath = "/user/bdss2001/carlafranca/stm"

  //Delete STM folder/file and recreate it
  val filesFolder = new Path(s"$filesPath")
  fs.delete(filesFolder, true)
  fs.mkdirs(filesFolder)

  //Add data files to stm
  val dataFiles = Array("trips.txt", "routes.txt", "calendar.txt")
  dataFiles.foreach(file => {
    fs.copyFromLocalFile(new Path(s"$path/data/$file"), new Path(filesPath))
  })

  //Delete course3 folder/files and recreate it
  val distFolder = new Path(s"$distPath")
  fs.delete(distFolder, true)
  fs.mkdirs(distFolder)

  //Read Data Routes
  val routeStream = fs.open(new Path(s"$filesPath/routes.txt"))
  val routes: List[Route] = Source
    .fromInputStream(routeStream)
    .getLines()
    .toList
    .tail.map(Route(_))

  //Read Data Calendar
  val calendarStream = fs.open(new Path(s"$filesPath/calendar.txt"))
  val calendars: List[Calendar] = Source
    .fromInputStream(calendarStream)
    .getLines()
    .toList
    .tail.map(Calendar(_))

  //Read Data Trips
  val tripStream = fs.open(new Path(s"$filesPath/trips.txt"))
  val trips: Iterator[String] = Source.fromInputStream(tripStream).getLines()

  //Instantiate Lookups
  val routeLookup = new RouteLookup(routes)
  val calendarLookup = new CalendarLookup(calendars)

  //Create csv file with header
  val header = "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible," +
    "route_long_name,route_color,date,exception_type\n"

  val fileName = new Path(s"$distPath/project.csv")
  val output = fs.create(fileName)
  output.writeChars(header)

  //Populate outputStream
  trips.next()
  while (trips.hasNext) {
    val line = trips.next()
    val trip = Trip(line)
    val tripRoute: TripRoute = TripRoute(trip, routeLookup.lookup(trip.routeId))
    val enrichedTrip: EnrichedTrip = EnrichedTrip(tripRoute.trip, tripRoute.route, calendarLookup.lookup(tripRoute.trip.serviceId))
    val t = Trip.toCsv(enrichedTrip.trip)
    val r = Route.toCsv(enrichedTrip.route)
    val c = Calendar.toCsv(enrichedTrip.calendar)
    output.writeChars(s"$t,$r,$c\n")
  }

  //Close streams
  output.flush()
  output.close()
  routeStream.close()
  tripStream.close()
  calendarStream.close()
}

case class TripRoute(trip: Trip, route: Option[Route])
case class EnrichedTrip(trip: Trip, route: Option[Route], calendar: Option[Calendar])