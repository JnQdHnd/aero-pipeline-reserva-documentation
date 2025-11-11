# Airbyte

Para llevar adelante el Pipeline utilizaremos Airbyte para conectar la Base de Datos de Origen (BDO) que es SQL Server con la Base de Datos de Destino (BDD) que es PostgreSQL.

Para llevar a cabo el procedimiento de migrado de datos de una fuente a la otra es necesario lo siguiente:

#####1. Crear una conexión con la BDO para lo cual utilizaremos la siguiente configuracion **(EN EL BLOQUE DE ABAJO DEBEMOS INGRESAR LA CONFIGURACION DE LA CONEXION REAL)**:

```json
 {
  "name": "Microsoft SQL Server (MSSQL)",
  "workspaceId": "d3fe3b4b-b7e5-4ea8-ae5a-22615e869c3b",
  "definitionId": "b5ea17b1-f170-46dc-bc31-cc744ca984c1",
  "configuration": {
    "host": "host.docker.internal",
    "port": 1433,
    "schemas": [
      "dbo"
    ],
    "database": "Aero-Test",
    "password": "******",
    "username": "JnQd",
    "ssl_method": {
      "ssl_method": "encrypted_trust_server_certificate"
    },
    "tunnel_method": {
      "tunnel_method": "NO_TUNNEL"
    },
    "replication_method": {
      "method": "STANDARD",
      "exclude_todays_data": false
    }
  }
}
```


