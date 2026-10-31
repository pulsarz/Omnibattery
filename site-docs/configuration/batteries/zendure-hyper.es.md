# Zendure Hyper 2000 (MQTT)

El Hyper 2000 no tiene API HTTP local, así que Omnibattery se comunica con él por MQTT a través de un broker de tu propia red. Puedes usar el broker que ya utiliza la integración MQTT de Home Assistant o conectar Omnibattery directamente a otro, por ejemplo un contenedor de Mosquitto.

## ¿Lo necesito?

| Modelo | Carga AC máxima | Descarga AC máxima | Solar visible para Omnibattery |
|---|---:|---:|---|
| Hyper 2000 | 1.200 W | 1.200 W (o el `inverseMaxPower` menor que informe el equipo) | FV total y ambas entradas MPPT |

**Úsalo si** tu Hyper 2000 publica en un broker que controlas. Para la serie SolarFlow 800 / 1600 / 2400 / 3000 / 4000, usa la [ruta HTTP local](zendure.md).

## Antes de empezar

1. **Lleva el Hyper a tu broker.** De fábrica solo habla con la nube de Zendure. Redirígelo una vez por Bluetooth, por ejemplo con *solarflow-bt-manager* o con la opción de MQTT local de la integración de Zendure para Home Assistant. Después publica en topics como `/ja72U0ha/<deviceId>/properties/report`.
2. **Comprueba el tráfico.** En MQTT Explorer (o `mosquitto_sub -t '/+/+/properties/report' -v`) deberías ver informes JSON del Hyper cada pocos segundos. La segunda parte del topic es la **product key** y la tercera el **ID de dispositivo**.
3. **Desactiva HEMS / el modo CT inteligente** en la app de Zendure para que no compita con las consignas de Omnibattery.

!!! note "Mosquitto en Docker"
    Las dos rutas funcionan con tu propio contenedor. Si la integración MQTT de Home Assistant ya se conecta a él, elige **Broker MQTT de Home Assistant**. Si no, elige **Broker propio** e introduce la IP del equipo que ejecuta el contenedor y el puerto publicado (normalmente `1883`), además del usuario y la contraseña de tu `password_file` de Mosquitto si lo usas. `localhost` solo funciona si Home Assistant usa la red del host.

## Cómo añadirlo

1. En el asistente de Omnibattery, elige **Zendure Hyper 2000 (MQTT)**.
2. Escribe un **Nombre** y deja vacío el **ID de dispositivo**: Omnibattery escucha 30 segundos y encuentra todos los Zendure del broker. Si hay uno nuevo lo toma directamente; si hay varios eliges de una lista con el ID, los packs, la capacidad y la carga de cada uno. Los Hyper ya añadidos no aparecen, así que la segunda de dos pilas se elige sola.
3. Elige el **Broker MQTT** y, si es propio, su host, puerto y credenciales.
4. Deja el **Método de control** en **Automatización** salvo que tu firmware lo ignore (ver abajo).
5. Ajusta los límites de potencia y estado de carga. No hay que introducir capacidad: Omnibattery la lee de los packs.

Omnibattery pide un informe completo al Hyper al enviar el formulario; el asistente solo continúa cuando responde.

### La capacidad se lee de los packs

El número de serie de cada pack indica su modelo y, con él, la capacidad nominal: AB1000 / AB1000S 0,96 kWh, AB2000 / AB2000S / AB2000X 1,92 kWh, AB3000 2,88 kWh. Un Hyper con 3 × AB2000 informa por tanto **5,76 kWh**, visible como **Battery Total Energy**. Un pack añadido se detecta en vivo; tras quitar uno, recarga la integración para que salga del total. Solo si no se reconoce el número de serie de un pack el asistente pide la capacidad, que entonces se usa en su lugar.

### Encontrar el ID de un dispositivo

Si la detección no encuentra un Hyper, lee su ID en el broker. En Home Assistant abre **Ajustes → Dispositivos y servicios → MQTT → Configurar**, escribe `/+/+/#` en **Escuchar un topic** y empieza a escuchar. Cada mensaje del Hyper llega en `/<productKey>/<deviceId>/...`; la tercera parte es el ID. Un Hyper inactivo puede tardar en decir algo; si lo reinicias publica enseguida.

## Qué verás

Los mismos controles y lecturas que en otras baterías Zendure: control automático, **Force Mode** / **Set Charge Power** / **Set Discharge Power** por software bajo **Battery Manual Control**, **Target SOC**, **Minimum SOC** y **Max Inverter Output**, SOC, potencia de batería, potencia solar (total y por entrada MPPT), temperatura y SOC, tensiones y temperatura por pack. La energía cargada y descargada se integra a partir de la potencia, porque el Hyper no informa contadores de energía.

## Si no funciona

| Síntoma | Causa probable | Qué revisar |
|---|---|---|
| «No se puede conectar al broker MQTT» | Host, puerto o credenciales incorrectos, o el puerto del contenedor no está publicado | Conéctate con MQTT Explorer desde otro equipo con los mismos datos |
| «Solo se oyeron dispositivos Zendure ya configurados» | El otro Hyper estaba inactivo y no envió nada en 30 s, o no está redirigido a este broker | Introduce su ID (ver abajo) o reinicia ese Hyper y vuelve a enviar; habla justo al reconectarse |
| «Ningún dispositivo Zendure ha publicado en este broker» | El Hyper sigue hablando con la nube de Zendure | Repite la redirección por Bluetooth; revisa los topics con MQTT Explorer |
| «El dispositivo no ha respondido» | ID incorrecto, o el broker reenvía informes pero no órdenes | Copia el ID del topic; un bridge debe reenviar `iot/<productKey>/<deviceId>/#` **hacia** el equipo |
| La potencia cambia un momento y vuelve al valor de la app | HEMS / modo CT inteligente activado | Desactívalo en la app de Zendure |
| Se ignoran las consignas | Firmware sin órdenes de automatización | Reconfigura la batería con **Método de control: Propiedades** |
| La batería queda no disponible al rato | El Hyper dejó de informar durante dos minutos | Revisa su Wi-Fi y el broker; se recupera solo cuando vuelven los informes |

??? "Detalles avanzados"
    **Topics.** Los informes llegan en `/<productKey>/<deviceId>/properties/report` (deltas). Omnibattery pide un volcado completo cada 30 segundos en `iot/<productKey>/<deviceId>/properties/read`, responde a la petición `time-sync` del equipo y escribe la configuración en `.../properties/write`. Las product keys conocidas del Hyper 2000 son `ja72U0ha` y `gDa3tb`; cualquier otra se aprende del topic en el que responde el equipo.

    **Métodos de control.** *Automatización* envía la función `deviceAutomation` (`autoModel` 8) en `.../function/invoke`, la misma orden que usa el modo inteligente de la app; vive en RAM y se refresca cada minuto. *Propiedades* escribe `acMode` con `inputLimit` / `outputLimit` y `smartMode=1`, como el driver HTTP. Los cambios de configuración (límites de SOC, salida del inversor) se escriben como propiedades en ambos modos.

    **Potencia mínima y arranque.** El Hyper puede ignorar órdenes muy pequeñas, que Omnibattery leería como una batería que no entrega. Por eso el driver nunca envía menos de 50 W: una petición menor se redondea a 50 W y el control de red corrige la diferencia en segundos. Si un arranque pequeño (menos de 100 W) sigue sin producir potencia a los 10 segundos, se reenvía una vez a 100 W hasta que el Hyper arranca (como mucho 15 segundos) y después se restaura la petición. La integración oficial de Zendure usa la misma potencia de arranque de 50 W y el mismo empujón. **PD Min Charge Power** puede quedarse en 0.

    **Potencias a cero.** El Hyper omite las potencias que valen cero. Una potencia que no aparece durante unos 75 segundos se lee como 0, y las direcciones de carga y descarga son excluyentes, así que un informe que nombra una pone la otra a cero.

    **Unidades.** `socSet` y `minSoc` van en milésimas, las temperaturas en decikelvin y las tensiones de celda en centivoltios; todo se convierte.

    Para los controles comunes y los límites del sistema, consulta [Elegir la conexión de tu batería](index.md).
