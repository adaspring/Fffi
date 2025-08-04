import python

from Class cls, Function func
where cls.getName() = "Flask" and
      func.getName() = "route" and
      not func.getKwarg("methods").getValue().toString().matches("%CSRF%")
select func, "Route missing CSRF protection"
