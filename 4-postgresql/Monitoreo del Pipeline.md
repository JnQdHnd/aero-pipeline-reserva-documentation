# Monitoreo del Pipeline con Tableau

Dado que Tableau es la herramienta de uso final del pipeline y que la misma se utiliza en el cotidiano, consideramos una buena idea realizar desde allí mismo el monitoreo, haciéndolo cómodo, de fácil acceso e interpretación.

Para llevar adelante el monitoreo en Tableau trabajaremos con las siguientes bases de datos:

- Base de datos de SQL Server (Origen del Pipeline).

- Base de datos de PostgreSQL (Destino del Pipeline).

Y luego, desarrollar los siguientes paneles de visualización para monitorizar:

1. Ejecución de trabajos programados con SQL Agent.

2. Sincronizaciones de Airbyte.

3. Cantidad de datos migrados.

A continuación describiremos la elaboración de cada uno de ellos:

### 1. Ejecución de trabajos programados con SQL Agent

Como primer dato, es importante contemplar que para realizar este panel de control, es necesario tener realizado el [Punto 6](../2-sql-server/SQL%20Server.md#paso6) del documento SQL Server de este mismo manual. 

##### Conexión a Fuentes de datos:

Nos conectaremos a la BD de **SQL Server** y seleccionaremos la tabla **Log_RegistrarCambiosReserva.

##### Preparación de los datos:

Dentro del panel crearemos el Campo Calculado **Minutos desde última ejecución**:

```Tableu
DATEDIFF(
    'minute',
    MIN([Hora Inicio]),
    LOOKUP(MIN([Hora Inicio]), 1)
)*-1
```

Y el Campo Calculado **Estado de Ejecución**:

```Tableu
IF ISNULL([Minutos desde ultima ejecucion]) THEN "Inicial"
ELSEIF [Minutos desde ultima ejecucion] > 60 THEN "Falta ejecución"
ELSE "Exitosa"
END
```

##### Armado del panel:

Para armar el panel utilizaremos la visualización **Square**, en color utilizaremos el campo **Estado de Ejecución**, en **texto Minutos Desde Última Ejecución**. En las **filas** seleccionaremos **Hora Inicio** **y Minutos Desde Última Ejecución**.

El resultado esperado es similar al siguiente:

![](C:\Users\j_que\Documents\GitHub\aero-pipeline-reserva-documentation\imagenes\2025-12-16-08-17-55-image.png)

## 2. Sincronizaciones de Airbyte

##### Conexión a Fuentes de datos:

Para auditar el funcionamiento de las sincronizaciones de Airbyte será necesario conectar la BD de **PostgreSQL** de destino. Allí nos conectaremos a la tabla **jobs** que es creada automáticamente en la conexión de destino por Airbyte registrando fecha de **inicio **(created_at) y **fin** (updated_at) de las sincronizaciones, como así también el **estado** (status) de las mismas.

##### Preparación de los datos:

Para llevar a cabo este panel crearemos el campo calculado **Duración de Sincronización**:

```Tableu
DATETIME([Fin] - [Inicio])
```

##### Armado del panel:

Para armar el panel utilizaremos un gráfico de tipo **Barra**, colocaremos **Estatus como color**, **Inicio como columnas** y **Duración de Sincronización como filas**.

El resultado esperado es similar al siguiente:

![](C:\Users\j_que\Documents\GitHub\aero-pipeline-reserva-documentation\imagenes\2025-12-16-08-35-03-image.png)

## 3. Cantidad de datos migrados

En este panel monitorizaremos que la cantidad de datos migrados sea la correcta. Para ello deberemos corroborar la cantidad de datos en la BD de origen anteriores a la última actualización en la BD de destino y la cantidad de datos de la BD de destino y verificar que sean coincidentes.

##### Preparación previa de datos en SQL Server:

Para lograr el objetivo propuesto, será necesario crear una vista en SQL Server que nos permita tener la información necesaria para hacer la comparación. 

Para ello trabajaremos con la vista ya existente que unifica (en el caso de ejemplo de la tabla Reserva) Reserva y Reserva_Cambios, identificada como vw_Reserva_con_soft_delete; y crearemos otras necesarias para poder filtrar aquellos datos existentes en origen, pero que, por ser posteriores a la última sincronización y anteriores a la siguiente programada aún no se encuentran en destino.

El primer paso para esto será crear una vista similar a la mencionada anteriormente, pero que unificará los datos de Reserva, con los provenientes de la tabla interna de SQL Server de registro de Change Tracking. De este modo captaremos aquellos datos que aún no han sido incorporados en la tabla de cambios por el SQL Agent. 

```SQL
CREATE VIEW dbo.vw_Reserva_Con_Soft_Delete_CT
AS
WITH CT_All AS (
    SELECT
        CT.id_res,
        CT.SYS_CHANGE_VERSION,
        CT.SYS_CHANGE_OPERATION,
        CT.SYS_CHANGE_COLUMNS,
        CT.SYS_CHANGE_CONTEXT
    FROM CHANGETABLE(CHANGES dbo.Reserva, 0) AS CT
),
CT_Max AS (
    SELECT
        id_res,
        MAX(SYS_CHANGE_VERSION) AS max_version
    FROM CT_All
    GROUP BY id_res
),
CT_Latest AS (
    SELECT
        A.id_res,
        A.SYS_CHANGE_VERSION,
        A.SYS_CHANGE_OPERATION,
        A.SYS_CHANGE_COLUMNS,
        A.SYS_CHANGE_CONTEXT
    FROM CT_All AS A
    JOIN CT_Max AS M
      ON A.id_res = M.id_res
     AND A.SYS_CHANGE_VERSION = M.max_version
)
SELECT
    CT.id_res,

    R.id_cli, R.id_usu, R.sucursal, R.bruto, R.neto, R.fec_sal,
    R.cie_res, R.cie_cli, R.cie_ope, R.comis,
    R.er, R.num_pax, R.fec_ape, R.fec_com, R.ai, R.obse,
    R.nor_dev, R.id_mov, R.CIE_FAC, R.id_mov_com,
    R.iva_ven, R.gasto, R.comision, R.NOINSCRIPTO,
    R.detalle_viaje, R.cie_ope2, R.reserva_adm, R.fecha_cierre,
    R.brutous, R.netous, R.gastous, R.comisionus,
    R.detalle_cliente, R.cotizacion, R.iva_venus, R.id_depto,
    R.fecha_regreso, R.id_pro, R.impuestos, R.impuestosus,
    R.fecha_vto, R.id_usu_cierre, R.id_usu_apertura,
    R.comis2, R.id_operes, R.ID_USU2,
    CASE CT.SYS_CHANGE_OPERATION
        WHEN 'I' THEN 'INSERT'
        WHEN 'U' THEN 'UPDATE'
        WHEN 'D' THEN 'DELETE'
    END AS ultimo_tipo_cambio,
    GETDATE() AS update_at,
    CASE
        WHEN CT.SYS_CHANGE_OPERATION = 'D'
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
    END AS is_deleted,
    'CHANGE_TRACKING' AS origen_registro
FROM CT_Latest AS CT
LEFT JOIN BASE_CT.dbo.Reserva AS R
       ON R.id_res = CT.id_res;
```

Y luego crearemos una vista que se encargue de unificar y filtrar los datos provenientes de ambas vistas, para dejar todo listo para el análisis requerido. 

Esta última vista unificada, tomará como base los datos proveniente de vw_Reserva_con_soft_delete, pero a aquellos datos existentes en la tabla Reserva, pero que carecen de fecha de actualización por estar creados, actualizados o eliminados con fecha posterior a la ejecución de SQL Agent (y por ende no están en la tabla de registro de cambios); los tomará de la vista vw_Reserva_Con_Soft_Delete_CT y los identificará con fecha de hoy para poder filtrarlos como posteriores a la última actualización.

```SQL
CREATE VIEW dbo.vw_Reserva_Unificada
AS
WITH U AS (
    SELECT
        O.*,
        C.id_res AS ct_id_res,
        C.update_at AS ct_update_at,
        C.ultimo_tipo_cambio AS ct_ultimo_tipo_cambio,
        C.is_deleted AS ct_is_deleted,
        C.origen_registro AS ct_origen_registro,
        CASE
            WHEN C.id_res IS NOT NULL
             AND (O.id_res IS NULL OR O.update_at = '19000101')
            THEN 1
            ELSE 0
        END AS usar_ct
    FROM dbo.vw_Reserva_Con_Soft_Delete O
    FULL OUTER JOIN dbo.vw_Reserva_Con_Soft_Delete_CT C
           ON O.id_res = C.id_res
)
SELECT
    COALESCE(id_res, ct_id_res) AS id_res,
    CASE WHEN usar_ct = 1 THEN id_cli ELSE id_cli END AS id_cli,
    CASE WHEN usar_ct = 1 THEN id_usu ELSE id_usu END AS id_usu,
    CASE WHEN usar_ct = 1 THEN sucursal ELSE sucursal END AS sucursal,
    CASE WHEN usar_ct = 1 THEN bruto ELSE bruto END AS bruto,
    CASE WHEN usar_ct = 1 THEN neto ELSE neto END AS neto,
    CASE WHEN usar_ct = 1 THEN fec_sal ELSE fec_sal END AS fec_sal,
    CASE WHEN usar_ct = 1 THEN cie_res ELSE cie_res END AS cie_res,
    CASE WHEN usar_ct = 1 THEN cie_cli ELSE cie_cli END AS cie_cli,
    CASE WHEN usar_ct = 1 THEN cie_ope ELSE cie_ope END AS cie_ope,
    CASE WHEN usar_ct = 1 THEN comis ELSE comis END AS comis,
    CASE WHEN usar_ct = 1 THEN er ELSE er END AS er,
    CASE WHEN usar_ct = 1 THEN num_pax ELSE num_pax END AS num_pax,
    CASE WHEN usar_ct = 1 THEN fec_ape ELSE fec_ape END AS fec_ape,
    CASE WHEN usar_ct = 1 THEN fec_com ELSE fec_com END AS fec_com,
    CASE WHEN usar_ct = 1 THEN ai ELSE ai END AS ai,
    CASE WHEN usar_ct = 1 THEN obse ELSE obse END AS obse,
    CASE WHEN usar_ct = 1 THEN nor_dev ELSE nor_dev END AS nor_dev,
    CASE WHEN usar_ct = 1 THEN id_mov ELSE id_mov END AS id_mov,
    CASE WHEN usar_ct = 1 THEN CIE_FAC ELSE CIE_FAC END AS CIE_FAC,
    CASE WHEN usar_ct = 1 THEN id_mov_com ELSE id_mov_com END AS id_mov_com,
    CASE WHEN usar_ct = 1 THEN iva_ven ELSE iva_ven END AS iva_ven,
    CASE WHEN usar_ct = 1 THEN gasto ELSE gasto END AS gasto,
    CASE WHEN usar_ct = 1 THEN comision ELSE comision END AS comision,
    CASE WHEN usar_ct = 1 THEN NOINSCRIPTO ELSE NOINSCRIPTO END AS NOINSCRIPTO,
    CASE WHEN usar_ct = 1 THEN detalle_viaje ELSE detalle_viaje END AS detalle_viaje,
    CASE WHEN usar_ct = 1 THEN cie_ope2 ELSE cie_ope2 END AS cie_ope2,
    CASE WHEN usar_ct = 1 THEN reserva_adm ELSE reserva_adm END AS reserva_adm,
    CASE WHEN usar_ct = 1 THEN fecha_cierre ELSE fecha_cierre END AS fecha_cierre,
    CASE WHEN usar_ct = 1 THEN brutous ELSE brutous END AS brutous,
    CASE WHEN usar_ct = 1 THEN netous ELSE netous END AS netous,
    CASE WHEN usar_ct = 1 THEN gastous ELSE gastous END AS gastous,
    CASE WHEN usar_ct = 1 THEN comisionus ELSE comisionus END AS comisionus,
    CASE WHEN usar_ct = 1 THEN detalle_cliente ELSE detalle_cliente END AS detalle_cliente,
    CASE WHEN usar_ct = 1 THEN cotizacion ELSE cotizacion END AS cotizacion,
    CASE WHEN usar_ct = 1 THEN iva_venus ELSE iva_venus END AS iva_venus,
    CASE WHEN usar_ct = 1 THEN id_depto ELSE id_depto END AS id_depto,
    CASE WHEN usar_ct = 1 THEN fecha_regreso ELSE fecha_regreso END AS fecha_regreso,
    CASE WHEN usar_ct = 1 THEN id_pro ELSE id_pro END AS id_pro,
    CASE WHEN usar_ct = 1 THEN impuestos ELSE impuestos END AS impuestos,
    CASE WHEN usar_ct = 1 THEN impuestosus ELSE impuestosus END AS impuestosus,
    CASE WHEN usar_ct = 1 THEN fecha_vto ELSE fecha_vto END AS fecha_vto,
    CASE WHEN usar_ct = 1 THEN id_usu_cierre ELSE id_usu_cierre END AS id_usu_cierre,
    CASE WHEN usar_ct = 1 THEN id_usu_apertura ELSE id_usu_apertura END AS id_usu_apertura,
    CASE WHEN usar_ct = 1 THEN comis2 ELSE comis2 END AS comis2,
    CASE WHEN usar_ct = 1 THEN id_operes ELSE id_operes END AS id_operes,
    CASE WHEN usar_ct = 1 THEN ID_USU2 ELSE ID_USU2 END AS ID_USU2,
    CASE WHEN usar_ct = 1 THEN ct_update_at ELSE update_at END AS update_at,
    CASE WHEN usar_ct = 1 THEN ct_ultimo_tipo_cambio ELSE ultimo_tipo_cambio END AS ultimo_tipo_cambio,
    CASE WHEN usar_ct = 1 THEN ct_is_deleted ELSE is_deleted END AS is_deleted,
    CASE WHEN usar_ct = 1 THEN 'CHANGE_TRACKING' ELSE origen_registro END AS origen_registro
FROM U;
```

Con estas vistas creadas, ya podemos proceder a la conexión de las fuentes de datos en Tableau.

##### Conexión a Fuentes de datos:

En este caso, dado que tenemos que comparar los datos en las BD de origen y de destino, será necesario hacer una unión o blend de datos de dos fuentes:

a) En **SQL Server** nos conectaremos a la vista creada **vw_Reserva_Unificada**. 

b) En **PostgreSQL** nos conectaremos a la tabla de destino **vw_Reserva_con_soft_delete**.

Ambas tablas las conectaremos con una relación lógica de tipo exterior completo:

![](C:\Users\j_que\Documents\GitHub\aero-pipeline-reserva-documentation\imagenes\2025-12-16-09-51-13-image.png)

##### Preparación de los datos:

Para preparar los datos crearemos los siguientes campos calculados:

**1. Ultima actualización** (En destino)

```Tableau
{ MAX([update at (Test vw Reserva Test Con Soft Delete)]) }
```

**2. Mayor Id en Destino**

```Tableau
{MAX([id res (Test vw Reserva Test Con Soft Delete)])}
```

**3. Dato anterior a último envio**

```Tableau
IIF( [Update At] <= [Última actualización]
OR
([Update At] > [Última actualización] 
  AND [Ultimo Tipo Cambio] == "DELETE" 
  AND [Id Res] <= [Mayor_Id_Destino])
, 1, 0 )
```

**4. Total Origen en Fecha**

```Tableau
{SUM([Dato_anterior_a_ultimo_envio])}
```

**5. Total Destino**

```Tableau
{ FIXED : COUNT([id res (Test vw Reserva Test Con Soft Delete)])
```

**6. Datos por Enviar**

```Tableau
{ FIXED : SUM(IIF([Update At] > [Última actualización],1,0)) }
```

**7. Estado**

```Tableau
IF [Total Origen en fecha] != [Total Destino] THEN "¡Error!" ELSE "Ok" END
```

**8. Opcional: Crear etiquetas combinando texto y datos para la presentación**

Dejamos aquí un ejemplo que se debería replicar con: Total Origen, Total Destino, Datos sin enviar y Estado.

**Etiqueta total origen**

```
"   " + "Total origen: " + STR([Total Origen en fecha])
```

##### Armado del panel:

Para el armado del panel elegiremos el gráfico de tipo **Square**. Para **color** usaremos **Estado**. Como **texto** incorporaremos **Etiqueta Fecha, Etiqueta total origen, Etiqueta total destino, Etiqueta datos por enviar y Etiqueta estado**.

El resultado esperado es similar al siguiente:

![](C:\Users\j_que\Documents\GitHub\aero-pipeline-reserva-documentation\imagenes\2025-12-16-10-13-36-image.png)
