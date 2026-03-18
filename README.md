# Fraud Detection System 🔴

![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Scikit-learn](https://img.shields.io/badge/Scikit--learn-1.3-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-2.0-FF6600?style=for-the-badge)
![LightGBM](https://img.shields.io/badge/LightGBM-4.0-02B4A5?style=for-the-badge)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?style=for-the-badge&logo=jupyter&logoColor=white)

Привет! Это мой учебный проект по фрод аналитике. Я джун и только начинаю разбираться в этой теме, но постарался сделать что то полное и интересное.

Датасет попался большой - около 13 миллионов транзакций. Я решил разобраться можно ли самому найти мошеников без готовых меток, только через анализ поведения клиентов.

---

## Как я это делал

Проект шел в два этапа. Сначала SQL чтобы проверить гипотезы, потом Python + ML чтобы всё автоматизировать и расширить.

### Этап 1 - SQL анализ

Начинал с SQLite. Написал большой запрос через CTE где каждый блок это одна гипотеза про фрод. Нашел 4 паттерна:

- клиенты которые делают много транзакций за день в разных штатах
- мерчанты у которых больше 50% операций с ошибками
- клиенты у которых ночные суммы в 3 раза выше дневных
- пары быстрых транзакций с интервалом меньше 2 минут

```sql
with custAVG as (
    SELECT client_id,
    AVG(CAST(REPLACE(amount, '$', '') AS REAL)) as AVGTRANS
    from transactions_data
    where merchant_state is not null
    and merchant_city <> 'ONLINE'
    GROUP by client_id
),

CTE as (
    select client_id, count(*) as TRANSACTIONDAY,
    count(DISTINCT merchant_state) as uniq_trans,
    AVG(CAST(replace(amount, '$', '') AS REAL)) as avgperday
    from transactions_data
    where merchant_state is not null
    and merchant_city <> 'ONLINE'
    group by client_id, date(date)
),

task01 as (
    -- клиенты с аномальным числом транзакций за день
    select DISTINCT client_id, TRANSACTIONDAY, uniq_trans,
    round(avgperday / avgtrans, 2) as ratio_risk,
    case
        when TRANSACTIONDAY > 10 THEN 'Критический'
        when TRANSACTIONDAY > 5 Then 'Высокий'
        ELSE 'Средний'
    end as Risk
    from CTE
    join custAVG using(client_id)
    where uniq_trans > 1
    and AVGTRANS * 2 < avgperday
    and TRANSACTIONDAY > 10
),

-- ... остальные CTE по той же логике (night, fast_tx, bad merch)

SELECT
    t.client_id,
    MAX(CASE WHEN t1.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_velocity,
    MAX(CASE WHEN t2.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_merch,
    MAX(CASE WHEN t3.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_night,
    MAX(CASE WHEN t4.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_fast_tx,
    ( ... сумма флагов ... ) AS risk_score
FROM (SELECT DISTINCT client_id FROM transactions_data) t
LEFT JOIN task01 t1 ON t.client_id = t1.client_id
...
ORDER BY risk_score DESC
```

Полный SQL в файле `fraud_analysis.sql`

### Этап 2 - Python и ML

Переписал всё в pandas и добавил ещё 5 флагов которые сложнее делать в SQL. Итого вышло 9 флагов, потом обучил 5 моделей и сравнил их.

---

## Флаги фрода (9 штук)

| # | Флаг | Что проверяет |
|---|---|---|
| 1 | velocity | много транзакций за день + разные штаты |
| 2 | bad_merch | мерчанты с >40% ошибок |
| 3 | night_tx | ночные суммы в 3x выше дневных |
| 4 | fast_tx | две крупные транзакции за <4 минуты |
| 5 | online_spike | онлайн траты в 2x выше офлайн |
| 6 | weekend | в выходные тратит аномально много |
| 7 | high_merch | топ 5% по количеству уникальных мерчантов |
| 8 | amount_spike | одна транзакция в 10x выше обычного чека |
| 9 | cross_state | 4+ штата за один месяц |

---

## Результаты ML моделей

Данные сильно дисбалансированы - фрода всего 3.2%. Использовал SMOTE чтобы немного выровнять классы.

| Модель | AUC | Recall (фрод) | F1 |
|---|---|---|---|
| Logistic Regression | 0.739 | 0.75 | 0.33 |
| XGBoost | 0.736 | 0.62 | 0.38 |
| LightGBM | 0.722 | 0.60 | 0.34 |
| Gradient Boosting | 0.708 | 0.58 | 0.32 |
| Random Forest | 0.717 | 0.50 | 0.28 |

Precision низкий - это ожидаемо при таком дисбалансе. Зато Recall у лучшей модели 0.75, то есть 3 из 4 реальных фродеров находим. Считаю неплохо для учебного проекта.

---

## Схема пайплайна

```
RAW DATA (13M транзакций)
        |
        v
Предобработка
(парсинг дат и сумм, генерация признаков)
        |
        v
9 rule-based флагов
        |
        v
Risk Score (0-9) + Risk Level (Низкий/Средний/Высокий/Критический)
        |
        v
5 ML моделей + SMOTE балансировка
        |
        v
Anomaly Detection (Isolation Forest без разметки)
        |
        v
Финальный статус каждого клиента
```

---

## Итоговые статусы

В конце каждый клиент получает один статус на основе флагов и ML:

| Статус | Когда присваивается |
|---|---|
| Чистый | нет флагов, ML не нашел |
| Подозрительный | 1-2 флага |
| Высокий риск | 3-4 флага |
| ML-фрод | только ML нашел |
| Критический | 5+ флагов |
| Подтверждённый фрод | и флаги и ML согласны |

---

## Структура репозитория

```
fraud-detection/
|
|-- fraud_detection_full.ipynb   # основной ноутбук (12 блоков)
|-- fraud_analysis.sql           # SQL запросы первого этапа
|-- requirements.txt             # зависимости
|-- README.md
|-- data/
    |-- transactions_data.csv    # не загружен, скачать с Kaggle
```

---

## Как запустить

1. Клонировать репозиторий
```bash
git clone https://github.com/ТВО_ИМЯ/fraud-detection.git
cd fraud-detection
```

2. Установить зависимости
```bash
pip install -r requirements.txt
```

3. Скачать датасет с Kaggle и положить в `data/`

4. В ноутбуке в Блоке 3 поменять путь:
```python
PATH = r'data/transactions_data.csv'
```

5. Запустить:
```
Cell -> Run All
```

---

## Стек

- Python, Pandas, NumPy
- Scikit-learn, XGBoost, LightGBM
- imbalanced-learn (SMOTE)
- PyOD (Isolation Forest)
- Matplotlib, Seaborn
- SQLite (первый этап)

---

## Немного о себе

Я джун в аналитике данных, этот проект делал чтобы разобраться во фрод аналитике и попрактиковаться в ML. Если найдёте ошибки или есть советы - пишите, буду рад фидбеку!
