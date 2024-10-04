from connectTW import engine
import re
import sqlite3
import  time
#import locale
import pandas as pd
import numpy as np
#from datetime import datetime
import datetime
from sqlalchemy import  Table, String, Column,Date
from sqlalchemy import insert
import sqlalchemy



#locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
metadata = sqlalchemy.MetaData()

def num_azs(name_user):
       return re.findall(r'[-+]?\d+',name_user)[0]


def write(id_user,pokazaniya,df_dog,date_otch,date_time_otch):
    #global available_tables,relevant_table,_pokazaniya,sutochn,_date_otch,_date_time_otch,_df_dog
    _pokazaniya=pokazaniya
    _date_otch= date_otch
    _date_time_otch=date_time_otch
    _df_dog=df_dog
    #print(id_user,pokazaniya,df_dog,date_otch,date_time_otch)

    _df_dog=_df_dog.drop_duplicates(['Объект'], keep='last')
    _df_dog=_df_dog[_df_dog['Инд_телеграм'].isin([str(id_user)])]
    relevant_table=f"{_df_dog.iloc[0]['Номер_договора']}_{_df_dog.iloc[0]['Объект']}"

    if not sqlalchemy.inspect(engine).has_table(relevant_table):

        Table(relevant_table, metadata,
            Column('Показание', String(30)),
            Column('Расход_за_сутки', String(10)), Column('Дата', Date, unique = True),
            Column('Дата_время', String(50)), Column('Плательщик', String(50), nullable=False ),
            Column('Способ',String(5)), Column('Авто', String(5)))

        metadata.create_all(engine)


    with engine.connect() as conn:
        available_tables=pd.read_sql_query(f"SELECT `Дата`, `Показание`, `Расход_за_сутки`, `Дата_время`, `Авто`  FROM `{relevant_table}` ORDER BY `Дата` DESC LIMIT 30",conn)
    available_tables['Расход_за_сутки']=available_tables['Расход_за_сутки'].astype(int)
    #available_tables=query.fe
    #print(available_tables, 30000)
    d=available_tables.to_dict()
    try:

        _sum = sum(d['Расход_за_сутки'].values())
        # sum=d['Расход_за_сутки'].values()
        # for i in sum[0]:
        #     _sum += int(i)
        median = _sum//len(available_tables)
        #print(median)
    except ZeroDivisionError:
        pass

    r=0
    try:
        #print(date_otch,available_tables.iloc[0]['Дата'].strftime('%Y.%m.%d'))
        if date_otch != available_tables.iloc[0]['Дата'].strftime('%Y.%m.%d'):
            if relevant_table == '74070751005027_Усть-Катав, Заводская':
                sutochn=round(int(pokazaniya)*0.02 - int(available_tables.iloc[0]['Показание'])*0.02)
                if sutochn < 1:
                   text=f"""<b>Проверьте правильность введённых данных</b>\nРасход электроэнергии не должен быть меньше или ровняться нулю.
            """
                   return text
                elif sutochn > median * 2.5:
                   text = "подтверждение"
                   return text_mess,sutochn, median, relevant_table,available_tables.iloc[0],_df_dog,_pokazaniya
                return write_1(sutochn, relevant_table,available_tables.iloc[0],_df_dog,_pokazaniya,_date_otch,_date_time_otch)
            else:
                sutochn=int(pokazaniya) - int(available_tables.iloc[0]['Показание'])
                if sutochn < 1:
                    text=f"""<b>Проверьте правильность введённых данных</b>\nРасход электроэнергии не должен быть меньше или ровняться нулю.
                """
                    return text
                elif sutochn > median * 2.5:
                    text_mess = "подтверждение"
                    #print(44)
                    return text_mess,sutochn, median, relevant_table,available_tables.iloc[0],_df_dog,_pokazaniya
                return write_1(sutochn,  relevant_table,available_tables.iloc[0],_df_dog,_pokazaniya,_date_otch,_date_time_otch)

        elif date_otch == available_tables.iloc[0]['Дата'].strftime('%Y.%m.%d'):
            text=f"Показание за смену передавались {available_tables.iloc[0]['Дата_время']}"
            return text

    except IndexError:

        elpower = Table(relevant_table, metadata, autoload_with=engine)
        stmt = insert(elpower).values(Показание=_pokazaniya, Расход_за_сутки = '0', Дата=_date_otch,
        Дата_время=_date_time_otch,Плательщик= _df_dog.iloc[0]['Плательщик'], Способ=_df_dog.iloc[0]['Способ'] )
        conn = engine.connect()
        conn.execute(stmt)
        conn.commit()

        return '0'

    except sqlite3.IntegrityError:
        pass

def write_1(sutochn,  relevant_table,available_tables_id,_df_dog,_pokazaniya,_date_otch,_date_time_otch):
    #print(55)
    try:
        elpower = Table(relevant_table, metadata, autoload_with=engine)
        stmt = insert(elpower).values(Показание=_pokazaniya,
                                      Расход_за_сутки = sutochn,
                                      Дата=_date_otch,
                                      Дата_время=_date_time_otch,
                                      Плательщик= _df_dog.iloc[0]['Плательщик'],
                                      Способ=_df_dog.iloc[0]['Способ'] )
        conn = engine.connect()
        conn.execute(stmt)
        conn.commit()


        # (f"INSERT INTO `{relevant_table}` \
        # SET `Показание`='{_pokazaniya}',\
        # `Расход_за_сутки`= '{sutochn}', \
        # `Дата`='{_date_otch}',\
        # `Дата_время`='{_date_time_otch}',\
        # `Плательщик`='{_df_dog.iloc[0]['Плательщик']}', \
        # `Способ`='{_df_dog.iloc[0]['Способ']}'",engine)
        # engine.commit()

        try:
            procent=round(((sutochn-int(available_tables_id['Расход_за_сутки']))/int(available_tables_id['Расход_за_сутки']))*100, 1)
        except ZeroDivisionError:
            text="""В дальнешем, когда у меня будет больше информации, в ответ на Ваше сообщение я буду отправлять Вам информацию о количестве кВт израсходованных за смену и
    инфрмацию о том, на сколько больше или меньше было израсходовано электроэнергии в сравнение с предыдущей сменой.\n"""
            return text
        if available_tables_id['Авто'] == 'А':
            text=f"""<b>ПОКАЗАНИЯ ПРИНЯТЫ</b>\n<b>Информация по расходу эл.энергии за {(datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%d-%m-%Y')} не передавалась </b>
        Ежедневная передача показаний - ОБЯЗАТЕЛЬНА."""
            return text

        elif sutochn <= int(available_tables_id['Расход_за_сутки']):
            text=f"""<b>ПОКАЗАНИЯ ПРИНЯТЫ</b>\n<b>Информация по расходу эл.энергии</b>
        Израсходовано э/э за смену - <b>{sutochn} кВт/ч</b>
        что на <b>{procent-procent*2}%</b> меньше чем расход  за предыдущую смену."""
            return text

        elif sutochn > int(available_tables_id['Расход_за_сутки']):
            text=(f"""<b>ПОКАЗАНИЯ ПРИНЯТЫ</b>\n<b>Информация по расходу эл.энергии</b>
        Израсходовано э/э за смену - <b>{sutochn} кВт/ч,</b>
        что на <b>{procent}%</b> больше чем расход  за предыдущую смену.""")
            return text

    except sqlite3.IntegrityError:
        pass


def current_month(relevant_table_2):
    with engine.connect() as conn:
        df_dog_3 = pd.read_sql_query(f"SELECT Дата, Показание, Расход_за_сутки, Дата_время  FROM `{relevant_table_2}` ORDER BY Дата DESC LIMIT 31", conn)
    df_dog_3 = df_dog_3.iloc[::-1]
    df_dog_3['Дата'] = df_dog_3['Дата'].apply(lambda x:str(x)[0:7])
    df_dog_3=df_dog_3.drop_duplicates(['Показание'], keep='last')
    #print(df_dog_3)
    df_dog_3=df_dog_3.loc[df_dog_3['Дата']==datetime.datetime.today().strftime('%Y-%m')]
    df_dog_3=df_dog_3[['Дата_время','Показание','Расход_за_сутки']]
    df_dog_3.rename(columns = {'Дата_время':'Дата','Расход_за_сутки':'Расход'}, inplace = True )
    df_dog_3.index = np.arange(1, len(df_dog_3)+1)
    return df_dog_3


def last_month(relevant_table_2):
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    last_month.strftime("%Y.%m")
    with engine.connect() as conn:
        df_dog_3 = pd.read_sql_query(f"SELECT Дата, Показание, Расход_за_сутки, Дата_время  FROM `{relevant_table_2}` ORDER BY Дата DESC LIMIT 62", conn)
    df_dog_3 = df_dog_3.iloc[::-1]
    df_dog_3['Дата'] = df_dog_3['Дата'].apply(lambda x:str(x)[0:7])
    df_dog_3=df_dog_3.loc[df_dog_3['Дата']==last_month.strftime("%Y-%m")]
    df_dog_3=df_dog_3[['Дата_время','Показание','Расход_за_сутки']]
    df_dog_3.rename(columns = {'Дата_время':'Дата','Расход_за_сутки':'Расход'}, inplace = True )
    df_dog_3.index = np.arange(1, len(df_dog_3)+1)
    return df_dog_3


def df_tables():
    with engine.connect() as conn:
        return pd.read_sql_query ("SELECT table_name FROM information_schema.tables WHERE table_schema = 'db_enp';",conn )

def df_tbl_dog():
    with engine.connect() as conn:
        return pd.read_sql_query("SELECT Номер_договора,Номер_АЗС, Объект, Плательщик, Способ, Инд_телеграм  FROM Договор", conn)
