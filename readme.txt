1- Explicación del proceso general
    -Trate de utilizar SAS (mediante saspy) para la ejecución de procesos “core” (como el algoritmo 01) y de aquellas “querys” que realizaban el cálculo de las primas y los riesgos. Entiendo que esto se puede llevar adelante con “pyspark” y sus “dataframes” pero no considero entender lo suficiente el proceso como para realizar los cálculos por mi cuenta.
    -Los procesos que crean tablas con datos que era útiles para los cálculos finales los replique en “RDD” de “spark”.
    -No cree un “__init__” porque entiendo que no se va a ejecutar el proceso.

2- Lógica del flujo
    -En primer instancia monte un ambiente local de spark en mi equipo, pero luego de las aclaraciones de Karina, en el código replique una sesión de “Spark”.
    -El proceso “main” se ejecuta desde el modulo “process.py”
    -Todos los métodos que interactúan con spark se encuentran en el modulo “core\utils.py”
    -Si bien estuvo contemplado en primera instancia, no tuve tiempo de armar el set de pruebas unitarias correspondientes.

3- Retos encontrados
    -No tengo mucha experiencia trabajando con SAS y Spark. Intente replicar según mi criterio el armado de “data-frames” basados en “procedures” de SAS.
    -Se me hizo un poco complicado comprender el proceso de negocio y el armado del código sin la posibilidad de ejecutar y “debuggear” mediante pruebas unitarias el proceso.

4- Respuesta a las siguientes preguntas:
    o ¿Qué puedes inferir de las tablas insumo?
        -Las tablas insumo son vitales y son el pivote de todos los scripts para el cálculo de los indicadores posteriores.
    o ¿Qué puedes inferir de lo que hace el código a nivel funcional?
        -Es un proceso de devengamiento, que calcula primas tomando como referencia fechas que vienen como “input” y tipos de riesgos preestablecidos.
    o ¿Qué áreas de oportunidad detectaste en el código?
        -Veo viable generar un job automático que prepare todos los datos necesarios para el cálculo de las primas. Este job podría crear RDD/Dataframes de manera automática que luego podrían ser utilizados por el equipo encargado del procesamiento de datos.

5- En una escala del 1 al 10, donde 1 representa el grado más bajo y 10 el grado más alto de dificultad, ¿con qué nivel evalúas el ejercicio?
    -Considerando que no tengo tanta experiencia utilizando el stack tecnológico propuesto, no se si soy parámetro. Pero, a mi me resultó complejo, ya sea por lo mencionado anteriormente y por el desconocimiento del proceso de negocios.
    -Entiendo que con un poco más de información y algún caso de referencia (pyspark) podría haber aportado más valor.
    o Conclusiones
        -Valoro mucho la experiencia y me gustaría contar con más conocimientos para sentirme más cómodo en el desarrollo de este tipo de tareas, 
        -Les agradezco la oportunidad y lamento no haberlo entregado a tiempo.
