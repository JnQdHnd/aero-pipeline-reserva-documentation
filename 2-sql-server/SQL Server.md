# **SQL Server**

En el desarrollo del Pipeline que servirá para migrar datos de SQL Server a la Base de Datos unificada PostgrSQL, es necesaria una preparación de los datos en la fuente de origen (SQL Server).

Para esto se deberán llevar a cabo una serie de pasos:

1. [**Habilitación de Change Tracking** ](#paso1)

2. [**Creación de la tabla para el registro de cambios**](#paso2)

3. [**Creación del Procedimiento Almacenado**](#paso3)

4. [**Creación y calendarización con SQL Agent del Trabajo que ejecutará el Procedimiento Almacenado**](#paso4)

5. [**Creación de la vista que integra la tabla Reservas con la tabla Reserva_Cambios**](#paso5)

6. [**OPCIONAL: Incorporación de controles (Latencia y filas procesadas)**](#paso6)

###### *Utilizaremos como ejemplo a continuación, el proceso para habilitar el Pipeline de la tabla Reserva. El mismo proceso se deberá seguir para las demás tablas que se quieran incluir en el Pipeline.*

### <a name="paso1">1. Habilitar Change Tracking</a>

Dentro del SQL Server, se deberá ejecutar la siguiente consulta SQL. Hay que hacerlo dentro de la base de datos en la que se encuentra/n las tablas que queremos incluir en el Pipeline. Dado que no tenemos definido en este punto la base de datos exacta en la que trabajaremos, para el ejemplo definiremos como nombre de la misma BaseDeDatos.

```SQL
ALTER DATABASE BaseDeDatos -- Reemplazar <BaseDeDatos> por el nombre de la base de datos
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);

ALTER TABLE dbo.Reserva -- Indicar el nombre y la ubicación de la Tabla con la que se va a trabajar
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
```

#### 

### <a name="paso2">2. Crear tabla de registro de cambios</a>

Al habilitar Change Tracking, SQL Server deja un registro de los Insert, Delete y Update que se producen en las tablas en las que lo hayamos habilitado. Ese registro es temporal y se guarda como metadata. Para poder utilizarlo en el pipeline, debemos extraer esta información y registrarla en una nueva tabla de registro de cambio.

Dentro de nuesta BaseDeDatos debemos crear entonces la tabla donde haremos estos registros. Para ello, ejecutaremos la siguiente consulta SQL:

```SQL
CREATE TABLE [BaseDeDatos].dbo.Reserva_Cambios (
    id_res INT PRIMARY KEY,
    tipo_cambio NVARCHAR(10),
    fecha_cambio DATETIME DEFAULT GETDATE()
);
```

### 

### <a name="paso3">3. Crear el Procedimiento Almacenado (Store Procedure)</a>

Para que los datos registrados por el Change Tracking en la metadata se guarden definitivamente dentro de la tabla recién creada Reserva_Cambios, es necesario llevar a cabo la creación de un Procedimiento Almacenado que luego será ejecutado periódicamente por el SQL Agent.

El primer paso será crear una tabla donde se irá guardando la última versión consultada del Change Tracking:

```SQL
CREATE TABLE [BaseDeDatos].dbo.Reserva_ChangeVersion (
    ultima_version bigint NULL
);
```

E inicializar la tabla con valor cero:

```SQL
INSERT INTO [BaseDeDatos].dbo.Reserva_ChangeVersion
(ultima_version)
VALUES(0);
```

Luego procedemos a crear el Store Procedure propiamente dicho.

> **Aclaración**: Este Store Procedure **no incluye el control de latencia**. Si se quisiera implementar dicha versión **ver el [punto 6](#paso6b)**.

Para ello, debemos ejecutar la siguiente consulta SQL que creará el procedimiento sp_RegistrarCambiosReserva:

```SQL
CREATE   PROCEDURE dbo.sp_RegistrarCambiosReserva
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ultima_version BIGINT;
    SELECT @ultima_version = ultima_version FROM dbo.Reserva_ChangeVersion;

    DECLARE @nueva_version BIGINT = CHANGE_TRACKING_CURRENT_VERSION();

    MERGE dbo.Reserva_Cambios AS T
    USING (
        SELECT 
            CT.id_res,
            CASE 
                WHEN CT.SYS_CHANGE_OPERATION = 'I' THEN 'INSERT'
                WHEN CT.SYS_CHANGE_OPERATION = 'U' THEN 'UPDATE'
                WHEN CT.SYS_CHANGE_OPERATION = 'D' THEN 'DELETE'
            END AS tipo_cambio,
            GETDATE() AS fecha_cambio
        FROM CHANGETABLE(CHANGES dbo.Reserva, @ultima_version) AS CT
    ) AS S (id_res, tipo_cambio, fecha_cambio)
    ON T.id_res = S.id_res
    WHEN MATCHED THEN 
        UPDATE SET 
            T.tipo_cambio = S.tipo_cambio,
            T.fecha_cambio = S.fecha_cambio
    WHEN NOT MATCHED THEN
        INSERT (id_res, tipo_cambio, fecha_cambio)
        VALUES (S.id_res, S.tipo_cambio, S.fecha_cambio);

    UPDATE dbo.Reserva_ChangeVersion
    SET ultima_version = @nueva_version;
END;
```

##### ¿Qué hace exactamente esta consulta?:

###### **1️⃣ Inicio y configuración**

`CREATE PROCEDURE dbo.sp_RegistrarCambiosReserva AS BEGIN     SET NOCOUNT ON;`

🔹 Crea el procedimiento `sp_RegistrarCambiosReserva`.  
🔹 `SET NOCOUNT ON` evita que SQL Server devuelva el número de filas afectadas tras cada operación, mejorando el rendimiento en procesos automáticos (triggers, jobs, etc.).

---

###### **2️⃣ Recupera la última versión registrada**

`DECLARE @ultima_version BIGINT; SELECT @ultima_version = ultima_version FROM dbo.Reserva_ChangeVersion;`

🔹 La tabla `dbo.Reserva_ChangeVersion` guarda **la última versión de cambio que ya se procesó**.  
🔹 Esto sirve para **no repetir cambios antiguos**.  
🔹 Ejemplo: si la última versión registrada fue la 1050, este procedimiento sólo buscará cambios posteriores a esa versión.

---

###### **3️⃣ Obtiene la versión actual del Change Tracking**

`DECLARE @nueva_version BIGINT = CHANGE_TRACKING_CURRENT_VERSION();`

🔹 La función `CHANGE_TRACKING_CURRENT_VERSION()` devuelve **el número de versión actual** del sistema de *Change Tracking*.  
🔹 Ese número se incrementa cada vez que hay un cambio en cualquier tabla con *Change Tracking habilitado*.

🔹 En otras palabras:

> “Esto marca hasta dónde quiero revisar los cambios nuevos desde la última ejecución.”

---

###### **4️⃣ Consulta los cambios en la tabla `Reserva`**

`FROM CHANGETABLE(CHANGES dbo.Reserva, @ultima_version) AS CT`

🔹 `CHANGETABLE(CHANGES ...)` es una función de sistema que devuelve **las filas cambiadas** desde la versión `@ultima_version`.

Cada fila incluye:

- `id_res` → la clave primaria afectada,

- `SYS_CHANGE_OPERATION` → indica el tipo de cambio:
  
  - `'I'` → Insert
  
  - `'U'` → Update
  
  - `'D'` → Delete

---

###### **5️⃣ Mapea los resultados**

`SELECT     CT.id_res,    CASE         WHEN CT.SYS_CHANGE_OPERATION = 'I' THEN 'INSERT'         WHEN CT.SYS_CHANGE_OPERATION = 'U' THEN 'UPDATE'         WHEN CT.SYS_CHANGE_OPERATION = 'D' THEN 'DELETE'     END AS tipo_cambio,     GETDATE() AS fecha_cambio`

🔹 Traducen el código `'I'`, `'U'`, `'D'` a un texto legible (“INSERT”, “UPDATE”, “DELETE”).  
🔹 Agrega una marca temporal `fecha_cambio`.

---

###### **6️⃣ Sincroniza los resultados en `Reserva_Cambios`**

`MERGE dbo.Reserva_Cambios AS T USING ( ... ) AS S (id_res, tipo_cambio, fecha_cambio) ON T.id_res = S.id_res`

🔹 `MERGE` compara los registros nuevos (`S`) con los existentes en `dbo.Reserva_Cambios` (`T`) usando la clave `id_res`.

Y luego define tres posibles acciones 👇

###### a. Si ya existe (`WHEN MATCHED`)

`UPDATE SET     T.tipo_cambio = S.tipo_cambio,     T.fecha_cambio = S.fecha_cambio`

➡️ Actualiza el tipo y la fecha del cambio.

###### b. Si no existe (`WHEN NOT MATCHED`)

`INSERT (id_res, tipo_cambio, fecha_cambio) VALUES (S.id_res, S.tipo_cambio, S.fecha_cambio);`

➡️ Inserta un nuevo registro de cambio.

---

###### **7️⃣ Actualiza la versión procesada**

*(tu fragmento se cortó justo acá, pero el paso es este)*

`UPDATE dbo.Reserva_ChangeVersion SET ultima_version = @nueva_version;`

🔹 Registra que el sistema ya procesó todos los cambios **hasta la nueva versión actual**.  
🔹 Esto asegura que la próxima ejecución sólo capture los nuevos cambios.



> **Advertencia**: Si se está intentando crear el SP desde **DBeaver**, es necesario hacerlo entre bloques BEGIN/END explícitos, es decir, el script dejado arriba se debe utilizar entre:

```SQL
BEGIN
    EXEC('
        -- Aquí va el código SQL Server dejado arriba --
    ');
END;
```

### <a name="paso4">4. Creación y calendarización del Trabajo que ejecutará el Procedimiento Almacenado</a>

Como hemos visto en el paso previo, hemos creado un procedimiento para poblar la tabla Reserva_Cambios con las modificaciones que se vayan produciendo en la tabla principal. En este punto, es importante aclarar que dicho Procedimiento Almacenado no se ejecutará solo. Podríamos ejecutarlo manualmente, pero como el objetivo del proyecto es automatizar estos procesos, utilizaremos las herramientas que SQL Server nos provee para este tipo de situaciones. Está es SQL Agent y los Trabajos Programados (Jobs).

Procedamos entonces a crear el Trabajo Programado que tendrá una simple tarea, ejecutar sp_RegistrarCambiosReserva (nuestro Procedimiento Almacenado) cada 1 hora (O el lapso que nosotros queramos determinar). Para ello seguiremos los siguientes pasos:

1. Abrí **SQL Server Management Studio (SSMS)**.

2. En el árbol de la izquierda, expandí **SQL Server Agent → New... → Jobs ...**

3. En la pestaña **General**, nombralo por ejemplo:
   
   > `Actualizar_Cambios_Reserva`

4. En la pestaña **Steps → New...**
   
   - Step name: `RegistrarCambios`
   
   - Type: *Transact-SQL script (T-SQL)*
   
   - Database: `BaseDeDatos`
   
   - Command:
     
     `EXEC [BaseDeDatos].dbo.sp_RegistrarCambiosReserva;`

5. En **Schedules → New...**
   
   - Schedule name: `Cada hora`
   
   - Frequency: *Recurring*
   
   - Occurs every: `1 hour`
   
   - Start time: `00:00:00`

6. En **Notifications**, marcá:
   
   - “When the job fails” → enviar correo a un operador o grabar en el Event Log.

Para evitar que el job falle sin que lo notes:

1. En SQL Server Management Studio → `SQL Server Agent → Operators → New Operator...`
   
   - Name: `Alerta_DataSync`
   
   - E-mail: tu correo o el del área técnica.

2. Volvé a tu Job → pestaña **Notifications**
   
   - Activá “E-mail” → seleccioná `Alerta_DataSync`.
   
   - En condición: “When the job fails”.

### <a name="paso5">5. Creación de la Vista que integra Reservas con Reserva_Cambios</a>

El último paso será crear una vista para poder tener en una misma vista-tabla los datos de reserva, con los datos de timestamp y motivo del cambio que necesitaremos luego en el Pipeline para identificar datos actualizados y eliminados y registrarlos debidamente al final del Pipeline.

Para ello ejecutaremos el siguiente script:

```SQL
CREATE VIEW dbo.vw_Reservas_Para_Airbyte AS
SELECT 
    R.*,
    C.tipo_cambio,
    C.fecha_cambio
FROM [BaseDeDatos].dbo.Reserva AS R
LEFT JOIN [BaseDeDatos].dbo.Reserva_Cambios AS C
    ON R.id_res = C.id_res
    AND C.tipo_cambio IN ('INSERT', 'UPDATE');
```

Una vez realizados estos pasos, ya tenemos listas las dos fuentes de datos que enviaremos a través de Airbyte a PostgreSQL: vw_Reservas_Para_Airbyte y Reserva_Cambios.

### <a name="paso6">6. OPCIONAL: Incorporación de controles (Latencia y filas procesadas)</a>

Si se quisieran incluir los controles de latencia (duración de la ejecución del proceso) y filas procesadas, se deben seguir los siguientes pasos:

Crear la tabla de registro de logs:

```SQL Server
CREATE TABLE dbo.Log_RegistrarCambiosReserva
(
    id_log INT IDENTITY(1,1) PRIMARY KEY,
    fecha_inicio DATETIME2 NOT NULL,
    fecha_fin DATETIME2 NOT NULL,
    duracion_ms BIGINT NOT NULL,
    filas_procesadas INT NOT NULL,
    version_inicial BIGINT NOT NULL,
    version_final BIGINT NOT NULL,
    nombre_sp SYSNAME NOT NULL
);
```

<a name="paso6b">Y luego modificar o crear (Si se hubiese decidido crear el procedimiento con los logs desde el comienzo) el Store Procedure para que realice el registro de los tiempos de ejecución y las filas procesadas:</a>

```SQL Server
CREATE PROCEDURE dbo.sp_RegistrarCambiosReservaTestConLogs
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @inicio DATETIME2 = SYSDATETIME(),
        @fin DATETIME2,
        @filas INT,
        @ultima_version BIGINT,
        @nueva_version BIGINT,
        @duracion_ms BIGINT;

    -- Obtener versión inicial
    SELECT @ultima_version = ultima_version 
    FROM dbo.Reserva_Test_ChangeVersion;

    -- Nueva versión
    SET @nueva_version = CHANGE_TRACKING_CURRENT_VERSION();

    -- Contar filas
    SELECT @filas = COUNT(*)
    FROM CHANGETABLE(CHANGES dbo.Reserva_Test, @ultima_version) AS CT;

    ---------------------------------------------------------
    -- MERGE
    ---------------------------------------------------------
    MERGE dbo.Reserva_Test_Cambios AS T
    USING (
        SELECT 
            CT.id_res,
            CASE 
                WHEN CT.SYS_CHANGE_OPERATION = ''I'' THEN ''INSERT''
                WHEN CT.SYS_CHANGE_OPERATION = ''U'' THEN ''UPDATE''
                WHEN CT.SYS_CHANGE_OPERATION = ''D'' THEN ''DELETE''
            END AS tipo_cambio,
            GETDATE() AS fecha_cambio
        FROM CHANGETABLE(CHANGES dbo.Reserva_Test, @ultima_version) AS CT
    ) AS S (id_res, tipo_cambio, fecha_cambio)
    ON T.id_res = S.id_res
    WHEN MATCHED THEN 
        UPDATE SET 
            T.tipo_cambio = S.tipo_cambio,
            T.fecha_cambio = S.fecha_cambio
    WHEN NOT MATCHED THEN
        INSERT (id_res, tipo_cambio, fecha_cambio)
        VALUES (S.id_res, S.tipo_cambio, S.fecha_cambio);

    -- Actualizar versión procesada
    UPDATE dbo.Reserva_Test_ChangeVersion
    SET ultima_version = @nueva_version;

    ---------------------------------------------------------
    -- Log
    ---------------------------------------------------------
    SET @fin = SYSDATETIME();
    SET @duracion_ms = DATEDIFF(ms, @inicio, @fin);

    INSERT INTO dbo.Log_RegistrarCambiosReservaTest
    (
        fecha_inicio,
        fecha_fin,
        duracion_ms,
        filas_procesadas,
        version_inicial,
        version_final,
        nombre_sp
    )
    VALUES
    (
        @inicio,
        @fin,
        @duracion_ms,
        @filas,
        @ultima_version,
        @nueva_version,
        ''sp_RegistrarCambiosReservaTestConLogs''
    );
END;
```
