# Framework for saving cache to MongoDB

```python
import traiding.cache.mongodb.client_realization as client_realization
```

## Для настройки тестов
1. Зайти на Mongo сервер
при разворачивании Mongo в контейнерах
```bash
docker exec -it mongodb sh
```
2. Зайти в mongosh
```bash
mongosh --port 27017  --authenticationDatabase "admin" -u "root" -p
```
3. Создать пользователя для тестов
```bash
use nntrade
db.createUser(
  {
    user: "unittestbot",
    pwd:  "unittestbot",
    roles: [ { role: "readWrite", db: "nntrade_unittest" }]
  }
)
```
4. указать глобальные переменные в тестах