# Framework for saving cache to MongoDB
## add to requirements
NNTrade.cache.mongodb @ git+https://git@github.com/NNTrade/MongoDB-cache.git#egg=NNTrade.cache.mongodb

## Imropt
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
use mongodb-cache-test
db.createUser(
  {
    user: "unittestbot",
    pwd:  "unittestbot",
    roles: [ { role: "readWrite", db: "mongodb-cache-test" }]
  }
)
```
4. указать глобальные переменные в тестах
