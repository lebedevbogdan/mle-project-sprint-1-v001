# mle-project-sprint1-v001

Общая цель проекта: создать базовое решение для предсказания стоимости квартир сервиса "Своя Избушка"

Проект разделен на 3 этапа:
1. Сбор данных
2. Очистка данных
3. Обучение модели

Решение 1 и 2 задания находятся в следующих папках и файлах:

[dags](dags) - папка с DAGами, где

- [dags/flats_price.py](dags/flats_price.py) - dag для 1 задания (flats_price);

- [plugins/steps](plugins/steps) - модуль Steps с кодом этапов dag 1 задания, и с кодом для отправки сообщений в telegram 

- [dags/flats_price_clean.py](dags/flats_price_clean.py) - dag для 2 задания (clean_flats_price)

..............................................................................................

Решение 3 задания находится в следующих папках и файлах:

[scripts](scripts) - папка с этапами DVC-пайплайна, где

- [scripts/data.py](scripts/data.py) - чтение данных;

- [scripts/fit.py](scripts/fit.py) - обучение модели;

- [scripts/evaluate.py](scripts/evaluate.py) - оценка модели

Фыйлы и папки, связанные с DVC:

[.dvc](.dvc)

[dvc.lock](dvc.lock)

[dvc.yaml](dvc.yaml)

[params.yaml](params.yaml)
