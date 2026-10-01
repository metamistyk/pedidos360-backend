# DSY1107 — Semana 04 — Sesión 3 — Backend Spring Boot

API mínima que funciona como **OAuth2 Resource Server** y valida Access Tokens JWT emitidos por Microsoft Entra ID.

## Requisitos

- Java 21.
- Maven 3.9+ o IntelliJ IDEA con Maven integrado.
- Tenant y App Registration de la API previamente configurados.
- Scope `Pedidos.Read` expuesto por la API.
- Access Tokens v2 (`requestedAccessTokenVersion: 2`).

## 1. Configurar

Editar:

`src/main/resources/application.properties`

Reemplazar:

- `PEGAR_AQUI_TENANT_ID`
- `PEGAR_AQUI_CLIENT_ID_API`

El Client ID debe ser **el de la API**, no el del frontend.

## 2. Ejecutar

Con Maven:

```bash
mvn spring-boot:run
```

O ejecutar `PedidosApiApplication` desde IntelliJ IDEA.

## 3. Pruebas sin Angular

### Endpoint público

`GET http://localhost:8080/api/publico`

Esperado: `200 OK`.

### Endpoint protegido sin token

`GET http://localhost:8080/api/pedidos`

Esperado: `401 Unauthorized`.

### Endpoint protegido desde Angular

Angular usa `MsalInterceptor` para agregar automáticamente:

`Authorization: Bearer <ACCESS_TOKEN>`

Si el JWT es válido, `aud` corresponde a esta API y `scp` contiene `Pedidos.Read`, se espera `200 OK`.
