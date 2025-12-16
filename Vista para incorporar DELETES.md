# **Vista para incorporación de DELETES**

Dado que Airbyte está configurado para funcionar de modo Incremental + deduped para migrar los datos de manera más eficiente, solo incorpora INSERTs y UPDATEs. Para poder registrar también los DELETEs migraremos dos tablas utilizando las vistas creadas en origen: 

1. TABLA PRINCIPAL: 
   
   La tabla que incluye los datos principales que deseamos analizar con los timestamp (fecha_cambio) y registro de tipo de cambio (tipo_cambio) incorporados.

2. REGISTRO DE CAMBIOS: 
   
   La tabla que registra los cambios que han sucedido en la tabla principal.

Con estas dos tablas ya en PostgreSQL, procederemos a integrar los datos en una vista con la finalidad de hacer un registro de tipo **soft delete**, es decir, incorporando una columna donde registraremos is_deleted como true o false según corresponda. Eso nos permitirá a la hora de realizar los análisis de datos optar por filtrar los datos eliminados o utilizarlos si fuera de utilidad. 

Para poder mantener un correcto registro de los datos eliminados, la vista en cuestión realizará dos procesos:

1. En caso de que exista un DELETE en la tabla de registro de cambios, pero que el dato no exista en la base principal porque fue eliminado antes de la migración, procederemos a incorporarlo con los únicos datos disponibles: id, fecha_cambio y is_deleted = true. El resto se las columnas figuraran como NULL.

2. En caso de que el dato haya sido eliminado con posterioridad a la migración, existirá en la tabla principal, por lo cual solamente registraremos la eliminación en la columna "is_deleted" como "true".

Como resultado de estos procesos, tendremos los datos listos para procesar a gusto y placer.

El script para llevar adelante esta vista en caso de ejemplo de reservas es el siguiente:

```PL/pgSQL
-- En este caso el nombre corresponde a la vista 
-- para la tabla principal Reserva.
CREATE OR REPLACE VIEW public.vw_reservas_test_con_soft_delete AS
WITH ultimo_cambio AS (
    SELECT DISTINCT ON (id_res) 
        id_res, 
        tipo_cambio, 
        updated_at AS fecha_cambio 
    FROM public."vw_Reserva_Test_Cambios_Airbyte" 
    ORDER BY id_res, updated_at DESC
)
SELECT 
    COALESCE(r.id_res, uc.id_res) AS id_res,
-- Todas las columnas de la tabla principal, en este caso de Reserva
    r.ai,
    r.er,
    r.neto,
    r.obse,
    r.bruto,
    r.comis,
    r.gasto,
    r.comis2,
    r.id_cli,
    r.id_mov,
    r.id_pro,
    r.id_usu,
    r.netous,
    r."CIE_FAC",
    r."ID_USU2",
    r.brutous,
    r.cie_cli,
    r.cie_ope,
    r.cie_res,
    r.fec_ape,
    r.fec_com,
    r.fec_sal,
    r.gastous,
    r.iva_ven,
    r.nor_dev,
    r.num_pax,
    r.cie_ope2,
    r.comision,
    r.id_depto,
    r.sucursal,
    r.fecha_vto,
    r.id_operes,
    r.impuestos,
    r.iva_venus,
    r.comisionus,
    r.cotizacion,
    r.id_mov_com,
    r."NOINSCRIPTO",
    r.impuestosus,
    r.reserva_adm,
    r.fecha_cierre,
    r.detalle_viaje,
    r.fecha_regreso,
    r.id_usu_cierre,
    r.detalle_cliente,
    r.id_usu_apertura,
    -- Información del último cambio
    uc.tipo_cambio AS ultimo_tipo_cambio,
    uc.fecha_cambio AS ultimo_fecha_cambio,
    -- Determinar si está eliminado
    CASE 
        WHEN uc.tipo_cambio = 'DELETE' THEN true
        ELSE false
    END AS is_deleted,
-- Para identificar el origen del registro
    CASE 
        WHEN r.id_res IS NOT NULL THEN 'EXISTENTE'
        WHEN r.id_res IS NULL AND uc.tipo_cambio = 'DELETE' THEN 'HISTORICO_ELIMINADO'
        ELSE 'SIN_CAMBIOS'
    END AS origen_registro
FROM public."vw_Reservas_Test_Para_Airbyte" r
FULL OUTER JOIN ultimo_cambio uc ON r.id_res = uc.id_res
-- Incluir todos los registros de reservas y los deletes históricos
WHERE (
    -- Todas las reservas existentes
    r.id_res IS NOT NULL
    -- O deletes históricos (cambios DELETE sin reserva correspondiente)
    OR (r.id_res IS NULL AND uc.tipo_cambio = 'DELETE')
);
```
