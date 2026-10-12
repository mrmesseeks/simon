# SEQ-ORD-003 - Получение заказа из Bitrix

**Назначение**

AS-IS сценарий получения заказа из Bitrix в OpenCart через Kafka-топик `prod.orders`. Диаграмма описывает подтверждённый на стороне OpenCart входящий технический контур для событий с `producer = bitrix`: создание заказа (`created`), последующее обновление (`updated`) и изменение статуса (`status_changed`).

**Начальная точка**

Bitrix публикует в `prod.orders` событие `created` с `producer = bitrix` и ключом `order_uuid`.

**Основные условия сценария**

* источник события - Bitrix (`producer = bitrix`);

* транспорт - Kafka, топик `prod.orders`;

* Kafka key - `order_uuid`;

* для `created` передаётся полный snapshot заказа; `oc_order_id` не обязателен, так как локальный ID OpenCart на момент создания ещё может отсутствовать;

* минимальный payload `created` включает данные клиента, товары, доставку, оплату, totals и итоговую сумму;

* входящий consumer OpenCart обрабатывает внешние события и не применяет события собственного producer `opencart`;

* идемпотентность обеспечивается по `event_id`, а порядок/конкурентные изменения одного заказа контролируются по `order_uuid` и `created_at`.

**Основной путь**

 1. Bitrix формирует событие `created` по заказу и публикует его в Kafka `prod.orders` с ключом `order_uuid`.

 2. `consumer-oc-order-event` получает событие из Kafka и валидирует envelope.

 3. Consumer передаёт событие в OpenCart через API `api/customer/order` с сервисным токеном.

 4. OpenCart проверяет envelope и передаёт его в `OrderApplyService`.

 5. `OrderApplyService` выполняет дедупликацию по `event_id`, блокировку обработки по `order_uuid` и проверку актуальности события по `created_at`.

 6. OpenCart резолвит связанные данные: клиента по `sso_user_id`, группу клиента, статус заказа и другие канонические значения, используемые при записи.

 7. Если заказа с таким `order_uuid` в OpenCart нет, создаются Order, товары, опции и totals. При наличии статуса создаётся audit-запись в `order_history`.

 8. После успешной записи сохраняются sync-метаданные события, транзакция фиксируется.

 9. Изменения, применённые из Kafka, не публикуются обратно в `prod.orders`: на время входящего apply используется anti-loop `@sync_apply`.

10. OpenCart возвращает consumer результат обработки (`action = created` для нового заказа).

**Дальнейшие события Bitrix**

* `updated` - применяется к существующему заказу; переданные поля заменяются, отсутствующие в payload сохраняют текущее значение OpenCart. Если `products` или `totals` переданы явно, соответствующая коллекция заменяется целиком;

* `status_changed` - изменяет только статус заказа через `addOrderHistory`; остальные данные заказа не изменяются;

* `status_changed` для неизвестного `order_uuid` не создаёт новый заказ и считается неприменимым событием;

* `deleted` для заказов deprecated и игнорируется; отмена должна передаваться как изменение статуса.

{% diagram %}

@startuml
title SEQ-ORD-003 Заказ из Bitrix - AS-IS

autonumber

participant "Bitrix" as Bitrix
queue "Kafka\nprod.orders" as Kafka
participant "consumer-oc-order-event" as Consumer
participant "OpenCart API" as Api
participant "OrderApplyService" as Apply
database "OpenCart DB" as DB

== Создание заказа ==

Bitrix -> Kafka : created\nproducer = bitrix\nkey = order_uuid\nполный snapshot
Kafka -> Consumer : Получить событие created
Consumer -> Consumer : Валидация события
Consumer -> Api : POST api/customer/order\nX-Auth-Token
Api -> Api : Проверить envelope\nи producer
Api -> Apply : apply(envelope)
Apply -> Apply : Dedup по event_id\nlock по order_uuid\nпроверка created_at

alt Заказ с order_uuid отсутствует
    Apply -> Apply : Резолв клиента, группы,\nстатуса и справочников
    Apply -> DB : INSERT Order + products + totals
    Apply -> DB : Записать order_history\nпри наличии статуса
    Apply -> DB : Записать sync-метаданные
    Apply --> Api : action = created
else Заказ уже существует
    Apply -> DB : Применить snapshot\nк существующему Order
    Apply -> DB : Записать sync-метаданные
    Apply --> Api : action = updated
end

note over Apply,DB
Во время входящего apply установлен @sync_apply,
поэтому изменения не создают обратное Kafka-событие.
end note

Api --> Consumer : HTTP 200 + action

== Последующие изменения из Bitrix ==

opt Изменены данные заказа
    Bitrix -> Kafka : updated\nproducer = bitrix
    Kafka -> Consumer : updated
    Consumer -> Api : POST api/customer/order
    Api -> Apply : apply(envelope)
    Apply -> DB : Merge переданных данных\nотсутствующие поля сохраняются
end

opt Изменён статус заказа
    Bitrix -> Kafka : status_changed\ncanonical status
    Kafka -> Consumer : status_changed
    Consumer -> Api : POST api/customer/order
    Api -> Apply : apply(envelope)
    Apply -> DB : addOrderHistory()\nизменить только статус
end

note over Bitrix,DB
Бизнес-триггер создания заказа в Bitrix
и дальнейшее создание сделки AmoCRM
по этому пути требуют отдельного подтверждения.
end note

@enduml

{% enddiagram %}
