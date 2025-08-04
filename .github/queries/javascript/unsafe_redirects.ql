import javascript

from DataFlow::Node source, DataFlow::Node sink
where exists(DataFlow::path(source, sink)) and
      source instanceof RemoteFlowSource and
      sink instanceof LocationHrefSink
select sink, "Unsafe redirect using location.href"
