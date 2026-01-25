{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "7f90eeb2-6fa5-4083-8384-1c714ddf19b9",
   "metadata": {},
   "outputs": [],
   "source": [
    "from datetime import datetime\n",
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator\n",
    "import pandas as pd\n",
    "from sqlalchemy import create_engine\n",
    "engine = create_engine('postgresql://airflow:airflow@localhost:5433/airflow')\n",
    "import json"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "076678d3-fc4a-4b78-988f-e0274d51f8c1",
   "metadata": {},
   "outputs": [],
   "source": [
    "filename = 'dags/data/pets-data.json'\n",
    "def json_to_postgres():\n",
    "    with open(filename, 'r') as f:\n",
    "        data = json.load(f)\n",
    "    df = pd.json_normalize(data, 'pets')\n",
    "    df.to_sql('pets', \n",
    "              engine, \n",
    "              if_exists='replace',\n",
    "              index=False)\n",
    "    print('Таблица pets загружена')\n",
    "with DAG(\n",
    "    'pets_to_postgres',\n",
    "    start_date=datetime(2024, 1, 1),\n",
    "    schedule_interval='@once',  # Запустить один раз\n",
    "    catchup=False,\n",
    ") as dag:\n",
    "    \n",
    "    load_task = PythonOperator(\n",
    "        task_id='load_json_to_postgres',\n",
    "        python_callable=json_to_postgres\n",
    "    )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "470480bb-7886-4c6d-803a-062e1e2de524",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Таблица pets загружена\n"
     ]
    }
   ],
   "source": [
    "json_to_postgres()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "517bb627-58c9-4afc-a2b4-29dde80c4c57",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "59dc1b90-0557-4474-941a-241f6f1f0d67",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d1ff41ba-c424-4274-b6fc-dcec441cdf53",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e640ea94-ecb1-4578-a080-c7df190eeaf9",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.8"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
