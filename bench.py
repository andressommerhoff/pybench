from statistics import fmean, mode, stdev
from collections import defaultdict
import time
from pprint import pprint
import gc
from contextlib import ContextDecorator, contextmanager, suppress


default_timer = time.perf_counter_ns
ns_to_seconds = 1000000000
ns_to_ms = 1000000

__all__ = ["BenchReport"]


class BenchReport:
    """
    Clase para hacer reportes de tiempos de ejecución. Se utiliza como iterador en un for para repetir mediciones
    y como contexto de with para separar las distintas partes de un reporte. 
    
    Uso tipico simple:
    
        T1 = BenchReport("Medicion 1")   # Inicia Reporte
        for i in T1(10):                 # Declara que se realizaran 10 mediciones
            time.sleep(0.5)              # Codigo a hacer benchmark (pueden ser varias lineas)
        T1.report()                      # Reporte
    
    Uso tipico con separacion de partes (por ejemplo para separar medicion de Setup de medicion principal:
    
        T1 = BenchReport("Medicion 1")                      # Inicia Reporte
        for i in T1(10):                                    # Declara que se realizaran 10 mediciones
            with T1.part("Setup", report_apart=True):       # Inicia parte 1
                var1 = "Test"                               # Codigo a hacer benchmark (parte 1)
                time.sleep(0.5)                             # Codigo a hacer benchmark (parte 1)
            with T1(part="Load"):                           # Inicia parte 2
                var2 = var1 + "!"                           # Codigo a hacer benchmark (parte 2)
                time.sleep(0.8)                             # Codigo a hacer benchmark (parte 2)
        T1.report()                                         # Reporte que muestra tiempos separados de cada parte
    
    -----------------------------------------------------------
    
    Notas de variables internas de la clase:
    
    loop_benches = [deltatime_iter_1, deltatime_iter_2, deltatime_iter_3, ...]
    current_iteration  # <- Contiene el numero de la iteracion actual
    totallooptime      # <- Contiene el tiempo total de todas las iteraciones
    overhead           # <- KPI de totallooptime - sum(loop_benches) --> Suma de tiempo fuera de las iteraciones
                       #    (El ideal es que sea lo menor posible. 
                       #     Dividir por cantidad de iteraciones para tener dato unitario)
    
    """
        
    @staticmethod
    def _get_stats(measures):
        """
        Retorna estadisticas de la lista o iterador del parametro measures. Estadisticas son del tipo:
            {"total": 0.0, "n": 0, "avg": None, "min": None}
        """
        stat = {"total": 0.0, "n": 0, "avg": None, "min": None}
        
        # totaliza tiempo por parte
        for t in measures: 
            if t is not None:
                stat["total"] += t
                stat["n"] += 1
                if stat["min"] is None or stat["min"] > t:
                    stat["min"] = t

        stat["total"] = stat["total"] / ns_to_ms if stat["total"] is not None else None
        stat["min"] = stat["min"] / ns_to_ms if stat["min"] is not None else None
        
        # Obtiene tiempos promedios
        if stat["n"] > 0:
            stat["avg"] = stat["total"] / stat["n"]

        return stat 
    
    def __init__(self, name, repeat=1, _timer=default_timer, turn_gc_off=True): #, continues=None, level=None):
        self.name = name
        self._timer = _timer
        self.default_turn_gc_off = turn_gc_off
        self.default_repeat = repeat

        self.reset()

    def reset(self):
        self.parts_benches = {}
        self.apart_benches = {}
        self.loop_benches = []

        self.current_iteration = 0
        self.parts_names = set()
        self.apart_names = set()

        self._startlooptime = None
        self._lastlooptime = None
        self._endlooptime = None
        self.totaltime = None
        self.totalitertime = None
        self.overhead = None


    def __call__(self, n=None):
        """
        Inicia un iterador que realiza "n" ciclos de mediciones. Para ser usado como:
            >> BM = BenchReport("bench1")
            >> for _ in BM(100):
                    func_to_bench()
        """
        if n is not None:
            self.default_repeat = n
        self.reset()
        self._startlooptime = self._timer()
        # print(f"define loop {self._startlooptime}")
        return self

    def __iter__(self):
        self.current_iteration = 0
        self._lastlooptime = self._timer()
        #print(f"start loop {self.current_iteration} {self._lastlooptime}")
        return self

    def __next__(self):
        t2 = self._timer()
        delta = t2 - self._lastlooptime
        if self.current_iteration > 0:
            # La primera iteracion no tiene delta valida. Registra solo la siguiente.
            self.loop_benches.append(delta)
        self._lastlooptime = t2
        if self.current_iteration < self.default_repeat:
            self.current_iteration += 1
            #print(f"loop {self.current_iteration} {delta}")
            return self.current_iteration
        self.totaltime = self._lastlooptime - self._startlooptime
        self.totalitertime = sum(self.loop_benches)
        self.overhead = self.totaltime - self.totalitertime
        # print(f"stop {self.current_iteration} {self.totallooptime} overhead %{self.overhead/self.totallooptime*100:.1f} {self.overhead}")
        # print(self.loop_benches)
        raise StopIteration

    @contextmanager
    def part(self, partname=None, report_apart=False, turn_gc_off=None):
                
        gcold = gc.isenabled()
        if turn_gc_off is None:
            turn_gc_off = self.default_turn_gc_off
        if turn_gc_off:
            gc.disable()

        if report_apart:
            partname = "core" if partname is None else partname
            b = self.apart_benches
            p = self.apart_names
            b_anti = self.parts_benches
            p_anti = self.parts_names
        else:
            partname = "setup" if partname is None else partname
            b = self.parts_benches
            p = self.parts_names
            b_anti = self.apart_benches
            p_anti = self.apart_names
        
        # Chequeo que una parte no se el haya cambiado su report_apart previo (para evitar inconsistencia en resultados)
        if partname in p_anti:
            raise AttributeError("No allowed to change 'report_apart' to a previous 'partname'")
        else:
            p.add(partname)
                
        i = self.current_iteration
                    
        start_time = self._timer()
        #############################
        try:
            ###########################
            yield
            ###########################
        except Exception as e:
            if gcold:
                gc.enable()
            raise  # reraise exception
        else:        
            if gcold:
                gc.enable()
        ############################
        end_time = self._timer()
    
        delta = end_time - start_time
        
        # -------------------
        # Registra el tiempo
        # -------------------
        
        # Crea la iteracion tanto en benches como en apart_benches 
        if i not in b:
            b[i] = defaultdict(None)
        if i not in b_anti:
            b_anti[i] = defaultdict(None)
        
        # Agrega el tiempo delta
        if partname not in b[i] or b[i][partname] is None:
            b[i][partname] = delta
        else:
            b[i][partname] += delta   # Para misma iteracion suma el tiempo

    
    def iter_measures_part(self, part_or_apart):
        """
        Iterador que entrega los tiempos no Nulos de cada iteración para una parte o una apart en especifico.
        Nota: al ser un iterador no consume memoria guardando la informacion.
        """
        if part_or_apart in self.parts_names:
            is_apart = True
            b = self.parts_benches
        elif part_or_apart in self.apart_names:
            is_apart = False
            b = self.apart_benches
        else:
            raise KeyError(f"No existe parte o aparte con el nombre '{part_or_apart}'")
    
        # def inner():
        for i in b:
            if part_or_apart in b[i]:
                t = b[i][part_or_apart]
                if t is not None:
                    yield t
                    # return inner
    
    def iter_measures_parts_globalized(self, section):
        """
        Iterador que entrega el total del tiempo de cada iteración (considerando la section "parts", "aparts" o ambas "all")
        Nota: al ser un iterador no consume memoria guardando la informacion.
        """
        if section == "parts":
            b1 = self.parts_benches
            b2 = []
        elif section == "aparts":
            b1 = self.apart_benches
            b2 = []
        elif section == "all":
            b1 = self.parts_benches
            b2 = self.apart_benches
        else:
            raise KeyError(f"Parametro 'section' debe ser 'parts', 'aparts' o 'all'.")
    
        for i in b1:  # se puede usar i para ambos diccionarios.
            iter_t = None
            for p in b1[i]:
                t = b1[i][p]
                if t is not None:
                    iter_t = iter_t + t if iter_t else t
            if i in b2:
                for p in b2[i]:
                    t = b2[i][p]
                    if t is not None:
                        iter_t = iter_t + t if iter_t else t
            if iter_t is not None:
                yield iter_t

    
    def get_stats(self, as_pandas=True):
        if as_pandas:
            import pandas as pd
            if self.parts_names or self.apart_names:
                df = pd.concat([self.get_stats_parts(as_pandas), self.get_stats_total(as_pandas)])
            else:
                df = self.get_stats_total(as_pandas)
            return df
        else:
            data = self.get_stats_parts(as_pandas) + [{"typ": "TOTAL", "section": "TOTAL"} | self.get_stats_total(as_pandas)]
            return data
    
    def get_stats_total(self, as_pandas=True):
        """
        Retorna estadisticas de las iteraciones medidas. Estadisticas son del tipo:
            stats -> {"total": 0.0, "n": 0, "avg": None, "min": None}
        """
        data = self._get_stats(self.loop_benches)
        if as_pandas:
            import pandas as pd
            data = [{"typ": "TOTAL", "section": "TOTAL"} | data]
            df = pd.DataFrame(data, columns=["typ", "section", "total", "n", "avg", "min"]).set_index(["typ", "section"]).astype(float)
            return df
        return data
    
    def get_stats_parts(self, as_pandas=True):
        """
        Retorna estadisticas para cada parte y aparte, asi como calculos globales sumando todas las partes y apartes.
        Estadisticas son del tipo:
             stats -> {"total": 0.0, "n": 0, "avg": None, "min": None}
        Y vienen agrupadas en "parts", "aparts" y sus respectivas sections con sus subtotales y total
        """
        stat = self._get_stats

        parts = {p: stat(self.iter_measures_part(p)) for p in self.parts_names}
        aparts = {p: stat(self.iter_measures_part(p)) for p in self.apart_names}
        globalparts = stat(self.iter_measures_parts_globalized("parts"))
        globalaparts = stat(self.iter_measures_parts_globalized("aparts"))
        globalglobal = stat(self.iter_measures_parts_globalized("all"))

        data = (
            [{"typ": "parts",  "section": p} | parts[p] for p in parts] +
            [{"typ": "parts",  "section": "SUBTOTAL"} | globalparts] +
            [{"typ": "aparts", "section": p} | aparts[p] for p in aparts] +
            [{"typ": "aparts", "section": "SUBTOTAL"} | globalaparts] +
            [{"typ": "P+A",  "section": "TOTAL"} | globalglobal] 
        )

        if as_pandas:
            import pandas as pd
            df = pd.DataFrame(data, columns=["typ", "section", "total", "n", "avg", "min"]).set_index(["typ", "section"]).astype(float)
            return df
        
        return data

    def print_report(self):
        overhead_prc = None
        with suppress(TypeError):
            overhead_prc = self.overhead / self.totaltime * 100
            overhead_prc = f"{overhead_prc:.1f}"
        print("----------------------------------------------------------------------------")
        print(f"Report {self.name}")
        print(f"n:{self.current_iteration} total:{self.totaltime / ns_to_ms} loops:{self.totalitertime / ns_to_ms} overhead: {self.overhead / ns_to_ms} (%{overhead_prc})")
        print(f"first 10 loop measures: {self.loop_benches[:10]}")
        print("*************\nSTATS IN MS\n**************")
        pprint(self.get_stats())
        print("----------------------------------------------------------------------------\n")
        # pprint(self.get_stats_total())
        # if self.parts or self.apart_parts:
        #     print("\nSTATS PARTS")
        #     pprint(self.get_stats_parts())        



def prueba_decorator():
    t = 0 #0.01 #0.1
    n = 10

    T1 = BenchReport("Medicion 1")
    for i in T1(n):
        pass
    T1.print_report()
    
    import pandas as pd
    
    T2 = BenchReport("Medicion 2")
    for i in T2(n):
        df=pd.DataFrame([{"hola":33}])
    T2.print_report()
    
    T1 = BenchReport("Medicion 3")
    for i in T1(n):
        with T1.part("a"):
            pass
    T1.print_report()

    T1 = BenchReport("Medicion 4")
    for i in T1(n):
        with T1.part("a"):
            df=pd.DataFrame([{"hola":33}])
    T1.print_report()
    
    
    
if __name__ == '__main__':
    prueba_decorator()
    print("fin")
