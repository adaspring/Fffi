import javascript
import DataFlow

from Source source, Sink sink
where source instanceof RemoteFlowSource and
      sink instanceof DomXssSink and
      exists(DataFlow::path(source, sink))
select sink, "Potential DOM XSS vulnerability"
