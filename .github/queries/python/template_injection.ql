import python
import semmle.python.web.TemplateInjection

from TemplateString t, CallNode call
where call = t.getACall() and
      call.getFunc().(QualifiedName).getBaseName() = "render_template"
select call, "Potential template injection vulnerability"
