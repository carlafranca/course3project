package ca.mcit.bigdata.course3project.schema

case class Trip(
                 routeId: Int,
                 serviceId: String,
                 tripId: String,
                 tripHeadSign: String,
                 wheelchairAccessible: Boolean
                )

object Trip {

  def apply(inputLine: String): Trip = {
    val p = inputLine.split(",", -1)
    new Trip(p(0).toInt, p(1), p(2), p(3), if (p(6).toInt == 1) true else false)
  }

  def toCsv(trip: Trip): String = {
    s"${trip.routeId},${trip.serviceId},${trip.tripId},${trip.tripHeadSign},${trip.wheelchairAccessible}"
  }

}



