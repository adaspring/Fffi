import python
import semmle.python.web.XSS

from TemplateString t, Expr expr
where t.contains(expr) and
      not isSafeFilter(expr.getLastFilter()) and
      not isInSafeContext(expr)
select expr, "Potential XSS vulnerability (unescaped variable)"
