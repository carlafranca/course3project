package ca.mcit.bigdata.course3project.schema

case class Route(
                   routeId :Int,
                   routeLongName: String,
                   routeColor: String
                 )

object Route {

  def apply(inputLine: String): Route = {
    val fields = inputLine.split(",", -1)
    new Route(fields(0).toInt, fields(3), fields(6))
  }

  //Removed routeId because it exist on Trips
  def toCsv(route: Option[Route]): String = {
    route.map(r => s"${r.routeLongName},${r.routeColor}").getOrElse(",")

  }
}
