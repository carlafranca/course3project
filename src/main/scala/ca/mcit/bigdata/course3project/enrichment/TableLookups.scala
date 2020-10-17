package ca.mcit.bigdata.course3project.enrichment

import ca.mcit.bigdata.course3project.schema.{Calendar, Route}

// RouteLookup
class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[Int, Route] =
    routes.map(route => route.routeId -> route).toMap

  def lookup(routeId: Int): Option[Route] = lookupTable.get(routeId)
}

//CalendarLookup
class CalendarLookup(calendars: List[Calendar]) {
  private val lookupTable: Map[String, Calendar] =
    calendars.map(calendar => calendar.serviceId -> calendar).toMap

  def lookup(serviceId: String): Option[Calendar] = lookupTable.get(serviceId)
}
