package ca.mcit.bigdata.course3project.schema

case class Calendar(
                     serviceId :String,
                     date: String,
                     exceptionType: Int
                   )

object Calendar {

  def apply(inputLine: String): Calendar = {
    val fields = inputLine.split(",", -1)
    new Calendar(fields(0), fields(1), fields(2).toInt)
  }

  //Removed serviceId because it exist on trips
  def toCsv(calendar: Option[Calendar]): String = {
    calendar.map(c => s"${c.date},${c.exceptionType}").getOrElse(",")
  }

}
