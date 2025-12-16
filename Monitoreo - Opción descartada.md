# Opción descartada:

Crear tabla **pipeline_status_log**:

```SQL
CREATE TABLE pipeline_status_log (
    id SERIAL PRIMARY KEY,
    check_time TIMESTAMP DEFAULT now(),
    status_general TEXT,              -- OK | WARNING | ERROR
    status_sql_agent TEXT,            -- OK | ERROR
    status_airbyte TEXT,              -- OK | WARNING | ERROR
    status_trigger TEXT,              -- OK | WARNING | ERROR
    ultima_fecha_cambio TIMESTAMP,    -- desde reserva_cambios
    ultima_extraccion TIMESTAMP WITH TIME ZONE, -- desde vw_reservas_para_airbyte
    count_inconsistencias INT,        -- registros DELETE inconsistentes
    detalle TEXT                      -- texto opcional más descriptivo
);
```

Crear la función **fn_check_pipeline_health()** que verificará la salud de las operatorias: Ejecución del SQL Agent, Sincronización de Airbyte, Ejecución del Trigger en PostgreSQL.

```SQL
CREATE OR REPLACE FUNCTION public.fn_check_pipeline_health()
RETURNS void AS
$$
DECLARE
    v_ultima_cambio timestamp;
    v_ultima_extraccion timestamp with time zone;
    v_status_sql_agent text;
    v_status_airbyte text;
    v_status_trigger text;
    v_status_general text;
    v_count_inconsistencias int;
    v_detalle text;
BEGIN
    -- 1️⃣ Verificar SQL Agent (actualización en origen reflejada en reserva_cambios)
    SELECT MAX(fecha_cambio)
    INTO v_ultima_cambio
    FROM reserva_cambios;

    IF v_ultima_cambio IS NULL THEN
        v_status_sql_agent := 'ERROR';
    ELSIF now() - v_ultima_cambio > interval '2 hours' THEN
        v_status_sql_agent := 'ERROR';
    ELSE
        v_status_sql_agent := 'OK';
    END IF;

    -- 2️⃣ Verificar Airbyte (última extracción registrada)
    SELECT MAX(_airbyte_extracted_at)
    INTO v_ultima_extraccion
    FROM vw_reservas_para_airbyte;

    IF v_ultima_extraccion IS NULL THEN
        v_status_airbyte := 'ERROR';
    ELSIF now() - v_ultima_extraccion > interval '2 hours' THEN
        v_status_airbyte := 'WARNING';
    ELSE
        v_status_airbyte := 'OK';
    END IF;

    -- 3️⃣ Verificar consistencia del trigger de deletes
    SELECT COUNT(rc.id_res)
    INTO v_count_inconsistencias
    FROM reserva_cambios rc
    WHERE rc.tipo_cambio = 'DELETE'
      AND rc.id_res IN (
          SELECT v.id_res FROM vw_reservas_para_airbyte v
      );

    IF v_count_inconsistencias > 0 THEN
        v_status_trigger := 'WARNING';
    ELSE
        v_status_trigger := 'OK';
    END IF;

    -- 4️⃣ Determinar estado general
    IF v_status_sql_agent = 'ERROR' THEN
        v_status_general := 'ERROR';
        v_detalle := 'SQL Agent no actualiza reserva_cambios.';
    ELSIF v_status_airbyte = 'ERROR' THEN
        v_status_general := 'ERROR';
        v_detalle := 'Airbyte no sincroniza datos.';
    ELSIF v_status_airbyte = 'WARNING' OR v_status_trigger = 'WARNING' THEN
        v_status_general := 'WARNING';
        v_detalle := 'Pipeline con retraso o inconsistencias menores.';
    ELSE
        v_status_general := 'OK';
        v_detalle := 'Pipeline completo funcionando correctamente.';
    END IF;

    -- 5️⃣ Registrar todo en columnas separadas
    INSERT INTO pipeline_status_log (
        status_general,
        status_sql_agent,
        status_airbyte,
        status_trigger,
        ultima_fecha_cambio,
        ultima_extraccion,
        count_inconsistencias,
        detalle
    ) VALUES (
        v_status_general,
        v_status_sql_agent,
        v_status_airbyte,
        v_status_trigger,
        v_ultima_cambio,
        v_ultima_extraccion,
        v_count_inconsistencias,
        v_detalle
    );
END;
$$ LANGUAGE plpgsql;
```

**Creacion del Job para pgAgent:**

```SQL
-- ================================================
--  CREACIÓN DE JOB DE MONITOREO DEL PIPELINE
--  Base: Monitoreo_Pipeline (PostgreSQL 16)
--  Ejecuta cada 1 hora
-- ================================================

-- 1️⃣ Crear el Job principal
INSERT INTO pgagent.pga_job(
    jobid, jobjclid, jobname, jobdesc, jobhostagent, jobenabled
)
VALUES (
    DEFAULT,
    1,
    'Monitoreo_Pipeline_Airbyte',
    'Verifica la salud del pipeline SQL Server → Airbyte → PostgreSQL.',
    '',   -- se ejecuta en cualquier host del pgAgent
    TRUE
)
RETURNING jobid;
```

**Crear el paso del Job (el comando a ejecutar)**

Reemplazá **`<JOBID>`** por el número que devolvió el comando anterior 👇

```SQL
INSERT INTO pgagent.pga_jobstep (
    jstjobid, jstname, jstdesc, jstenabled, jstkind, jstconnstr, jstdbname, jstcode, jstonerror
)
VALUES (
    <JOBID>,
    'Ejecutar fn_check_pipeline_health',
    'Ejecuta la función de monitoreo y registra el estado en pipeline_status_log',
    TRUE,
    's',  -- tipo "SQL"
    '',   -- usa la conexión actual
    'Monitoreo_Pipeline',
    'SELECT public.fn_check_pipeline_health();',
    'f'   -- "Fail" si hay error
);
```

**Crear el Schedule (frecuencia de ejecución)**

También reemplazá `<JOBID>` por el mismo valor:

```SQL
INSERT INTO pgagent.pga_schedule (
    jscjobid,
    jscname,
    jscdesc,
    jscenabled,
    jscstart,
    jscminutes,
    jschours,
    jscweekdays,
    jscmonthdays,
    jscmonths
)
VALUES (
    1,
    'Cada 1 hora',
    'Ejecuta el control de salud del pipeline cada 60 minutos',
    TRUE,
    now(),
    ARRAY[
        false,false,false,false,false,false,false,false,false,false,
        false,false,false,false,false,false,false,false,false,false,
        false,false,false,false,false,false,false,false,false,false,
        false,false,false,false,false,false,false,false,false,false,
        true,false,false,false,false,false,false,false,false,false,
        false,false,false,false,false,false,false,false,false,false
    ],  -- minuto 40 (solo ese en true)
    ARRAY[
        true,true,true,true,true,true,true,true,true,true,
        true,true,true,true,true,true,true,true,true,true,
        true,true,true,true
    ],  -- todas las horas
    ARRAY[
        true,true,true,true,true,true,true
    ],  -- todos los días de la semana
    ARRAY[
        true,true,true,true,true,true,true,true,true,true,
        true,true,true,true,true,true,true,true,true,true,
        true,true,true,true,true,true,true,true,true,true,
        true,true
    ],  -- todos los días del mes
    ARRAY[
        true,true,true,true,true,true,true,true,true,true,true,true
    ]   -- todos lo
```
